"""Publish health-drop alerts to the MQTT broker after each gold cycle.

Edge-triggered: only fires when a repo's health_score crosses below the
configured threshold for the first time (not every cycle it remains low).
A broker outage is logged and silently ignored — it must never abort a
committed gold write.
"""

import json
import logging
import time

import paho.mqtt.client as mqtt

import config

log = logging.getLogger(__name__)


def publish_alerts(rows: list[dict], prev_scores: dict[str, str | None]) -> None:
    """Compare new vs. previous health scores; publish to MQTT on threshold crossing.

    Args:
        rows: Current gold output rows, each containing at least
              ``repo_name`` and ``health_score``.
        prev_scores: Mapping of repo_name → previous health_score string (or None
                     when the repo is new). Snapshot taken *before* Redis was updated.
    """
    threshold = config.MQTT_ALERT_THRESHOLD
    alerts: list[tuple[str, float, float | None]] = []

    for r in rows:
        repo = r.get("repo_name")
        raw_score = r.get("health_score")
        if not repo or raw_score is None:
            continue
        new_score = float(raw_score)
        if new_score >= threshold:
            continue
        prev_raw = prev_scores.get(repo)
        prev_score = float(prev_raw) if prev_raw is not None else None
        # Only fire on the crossing edge (or first appearance below threshold).
        if prev_score is None or prev_score >= threshold:
            alerts.append((repo, new_score, prev_score))

    if not alerts:
        return

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.connect(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT, keepalive=10)
        for repo, score, prev_score in alerts:
            owner, name = repo.split("/", 1)
            topic = f"repos/{owner}/{name}/alerts"
            payload = json.dumps({
                "repo": repo,
                "health_score": round(score, 4),
                "previous_health_score": round(prev_score, 4) if prev_score is not None else None,
                "threshold": threshold,
                "ts": int(time.time()),
            })
            client.publish(topic, payload, qos=1, retain=False)
            log.info("mqtt: alert published → %s (score=%.3f)", topic, score)
        client.disconnect()
    except Exception:
        log.exception("mqtt: failed to publish alerts (non-fatal)")
