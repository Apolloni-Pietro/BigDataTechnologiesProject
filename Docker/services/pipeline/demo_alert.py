"""Demo script: inject a fake health-score drop and fire an MQTT alert.

Run inside the pipeline container:
    docker compose exec pipeline python demo_alert.py
    docker compose exec pipeline python demo_alert.py --repo owner/repo --score 0.25 --prev 0.72

Arguments (all optional):
    --repo   OWNER/REPO    fake repo name          (default: demo-org/demo-repo)
    --score  FLOAT         new health_score        (default: 0.25, must be < threshold)
    --prev   FLOAT         previous health_score   (default: 0.85, must be >= threshold)
    --threshold FLOAT      override MQTT_ALERT_THRESHOLD from config

The script:
  1. Writes a "previous" entry to Redis so the alert shows a real score delta.
  2. Calls mqtt_alerts.publish_alerts() to fire the alert via the broker.
  3. Cleans up the fake Redis key afterwards.
"""

import argparse
import sys

import redis as redis_lib

import config
import mqtt_alerts


def main() -> None:
    parser = argparse.ArgumentParser(description="Fire a demo MQTT health alert.")
    parser.add_argument("--repo",      default="demo-org/demo-repo")
    parser.add_argument("--score",     type=float, default=0.25)
    parser.add_argument("--prev",      type=float, default=0.85)
    parser.add_argument("--threshold", type=float, default=None)
    args = parser.parse_args()

    threshold = args.threshold if args.threshold is not None else config.MQTT_ALERT_THRESHOLD

    if args.score >= threshold:
        print(f"ERROR: --score {args.score} must be below threshold {threshold} to trigger an alert.")
        sys.exit(1)
    if args.prev < threshold:
        print(f"WARNING: --prev {args.prev} is already below threshold {threshold}; "
              "the alert will still fire but won't represent a clean threshold crossing.")

    r = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)
    redis_key = f"latest:{args.repo}"

    # Seed Redis with the "previous" healthy score so the alert shows a real delta.
    r.hset(redis_key, mapping={
        "repo_name":    args.repo,
        "health_score": str(args.prev),
        "risk_score":   str(round(1.0 - args.prev, 4)),
    })
    print(f"Seeded Redis  {redis_key}  health_score={args.prev}")

    # Snapshot prev scores exactly as gold.build() does.
    prev_scores = {
        key.removeprefix("latest:"): r.hget(key, "health_score")
        for key in r.scan_iter("latest:*")
    }

    # Build a minimal fake row with the new (below-threshold) score.
    fake_row = {
        "repo_name":    args.repo,
        "health_score": args.score,
        "risk_score":   round(1.0 - args.score, 4),
    }

    print(f"Publishing alert: health_score {args.prev} → {args.score} "
          f"(threshold={threshold}) to repos/{args.repo}/alerts")
    mqtt_alerts.publish_alerts([fake_row], prev_scores)

    # Clean up the fake Redis key.
    r.delete(redis_key)
    r.close()
    print("Done. Fake Redis key cleaned up.")


if __name__ == "__main__":
    main()
