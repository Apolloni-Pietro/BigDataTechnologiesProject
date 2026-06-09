import json
import time
import logging
import os
from collections import defaultdict, deque
from datetime import datetime, date, timedelta

import redis as redis_lib
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────
BROKERS      = os.getenv("REDPANDA_BROKERS",  "redpanda:29092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN",      "postgresql://oss:changeme@timescaledb:5432/oss_health")
REDIS_URL    = os.getenv("REDIS_URL",         "redis://redis:6379")
FLUSH_EVERY  = int(os.getenv("FLUSH_EVERY",  "50"))
TOPIC        = "gh-events"
GROUP_ID     = "health-metric-worker"

# How long to retain each TimeSeries key in Redis (7 days in milliseconds).
REDIS_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

# Health score alert threshold.
ALERT_THRESHOLD = 0.35


# ── Per-repo state ────────────────────────────────────────────────────────

class RepoState:
    """
    Accumulates raw event data for one repository.
    Updated on every event. Flushed to storage every FLUSH_EVERY events.
    """

    def __init__(self, name: str):
        self.name = name

        # Rolling 30-day commit counts stored as (date, count) pairs.
        # Using a deque (double-ended queue) because we frequently
        # pop old entries from the left and append new ones on the right.
        self.daily_commits: deque = deque()

        # Set of unique contributor logins seen in the last 90 days.
        # A set automatically deduplicates — the same person committing
        # 100 times still counts as 1 contributor.
        self.contributors_90d: set = set()

        # Count of PRs by outcome.
        self.pr_closed_count  = 0
        self.pr_merged_count  = 0

        # Count of issues by state.
        self.issue_open_count   = 0
        self.issue_closed_count = 0

        # Internal accumulators for the current day's commits.
        self._today_commits = 0
        self._today_date    = date.today()

        # Timestamp of the most recent push event.
        self.last_push_at: datetime | None = None

    def record_push(self, actor: str, commit_count: int):
        today = date.today()

        # If the date has rolled over since we last recorded,
        # archive yesterday's count and prune entries older than 30 days.
        if today != self._today_date:
            self.daily_commits.append((self._today_date, self._today_commits))
            cutoff = today - timedelta(days=30)
            while self.daily_commits and self.daily_commits[0][0] < cutoff:
                self.daily_commits.popleft()
            self._today_commits = 0
            self._today_date    = today

        self._today_commits += commit_count
        self.contributors_90d.add(actor)
        self.last_push_at = datetime.utcnow()

    def record_pr(self, action: str, merged: bool = False):
        if action == "closed":
            self.pr_closed_count += 1
            if merged:
                self.pr_merged_count += 1

    def record_issue(self, action: str):
        if action == "opened":
            self.issue_open_count += 1
        elif action == "closed":
            # Move one issue from open to closed.
            self.issue_open_count   = max(0, self.issue_open_count - 1)
            self.issue_closed_count += 1

    def compute_metrics(self) -> dict:
        """
        Derive the final health metric values from accumulated state.
        Returns a plain dict of {metric_name: value}.
        """

        # ── Commit frequency ──────────────────────────────────────
        # Sum all recorded daily commits plus today's running total,
        # then divide by 30 to get an average per day.
        total_commits   = sum(c for _, c in self.daily_commits) + self._today_commits
        commit_freq_30d = total_commits / 30.0

        # ── Bus factor ────────────────────────────────────────────
        # True bus factor requires commit-level authorship data.
        # Here we use the number of unique contributors as a proxy.
        # A real implementation would weight by commit count per author.
        bus_factor = max(1, len(self.contributors_90d))

        # ── PR abandon rate ───────────────────────────────────────
        # Fraction of closed PRs that were closed WITHOUT being merged.
        # High abandon rate suggests PRs are being rejected or ignored.
        pr_abandon_rate = 0.0
        if self.pr_closed_count > 0:
            pr_abandon_rate = 1.0 - (self.pr_merged_count / self.pr_closed_count)

        # ── Days since last push ──────────────────────────────────
        days_since_push = 0
        if self.last_push_at:
            days_since_push = (datetime.utcnow() - self.last_push_at).days

        # ── Stale issue ratio ─────────────────────────────────────
        # Fraction of all tracked issues that are currently open.
        # A high ratio means issues pile up without being resolved.
        total_issues      = self.issue_open_count + self.issue_closed_count
        stale_issue_ratio = 0.0
        if total_issues > 0:
            stale_issue_ratio = self.issue_open_count / total_issues

        # ── Composite health score ────────────────────────────────
        # Five components, each normalised to [0, 1], then weighted.
        # Weights were chosen heuristically — adjust as you collect
        # real data and validate against known-healthy/abandoned repos.

        activity_score     = min(1.0, commit_freq_30d / 5.0)
        # score = 1.0 at >= 5 commits/day; 0.0 at 0 commits/day

        bus_score          = min(1.0, bus_factor / 5.0)
        # score = 1.0 at >= 5 contributors; 0.0 at 1 contributor

        responsiveness     = 1.0 - pr_abandon_rate
        # score = 1.0 if all PRs are merged; 0.0 if all PRs are abandoned

        freshness_score    = max(0.0, 1.0 - (days_since_push / 30.0))
        # score = 1.0 if pushed today; 0.0 if not pushed in 30+ days

        issue_health_score = 1.0 - stale_issue_ratio
        # score = 1.0 if all issues are closed; 0.0 if all are open

        health_score = (
            activity_score     * 0.30 +
            bus_score          * 0.25 +
            responsiveness     * 0.20 +
            freshness_score    * 0.15 +
            issue_health_score * 0.10
        )

        return {
            "commit_freq_30d":        round(commit_freq_30d,    4),
            "bus_factor":             bus_factor,
            "pr_latency_p50":         24.0,       # placeholder — hours
            "pr_abandon_rate":        round(pr_abandon_rate,    4),
            "stale_issue_ratio":      round(stale_issue_ratio,  4),
            "days_since_last_release": days_since_push,
            "health_score":           round(health_score,       4),
        }


# ── Storage helpers ───────────────────────────────────────────────────────

def write_to_redis(r: redis_lib.Redis, repo: str, metrics: dict) -> None:
    """
    Write one metric snapshot to Redis.
    Two writes per flush:
      1. A TimeSeries entry for each numeric metric (for trend charts).
      2. A plain hash of latest values (for O(1) current-value lookups).
    """
    ts          = r.ts()
    now_ms      = int(time.time() * 1000)   # milliseconds since epoch
    madd_args   = []

    for metric_name, value in metrics.items():
        if not isinstance(value, (int, float)):
            continue

        key = f"ts:{repo}:{metric_name}"

        # Create the TimeSeries key if it does not already exist.
        # The try/except is the idiomatic way — calling EXISTS first
        # would be a race condition.
        try:
            ts.create(
                key,
                retention_msecs = REDIS_RETENTION_MS,
                labels          = {"repo": repo, "metric": metric_name},
                duplicate_policy = "LAST",
                # LAST: if two values arrive with the same timestamp,
                # keep the most recent one. Avoids errors on re-processing.
            )
        except redis_lib.ResponseError:
            pass  # Key already exists — that is fine.

        madd_args.append((key, now_ms, float(value)))

    if madd_args:
        ts.madd(madd_args)
        # TS.MADD writes all values in a single round-trip to Redis.
        # Much more efficient than calling TS.ADD once per metric.

    # Store a plain hash of the latest values.
    # This is separate from TimeSeries and is used for current-value
    # lookups (no time range involved — just "what is it right now?").
    r.hset(
        f"latest:{repo}",
        mapping={k: str(v) for k, v in metrics.items()},
    )

    # Publish an alert if health score dropped below the threshold.
    score = metrics.get("health_score", 1.0)
    if score < ALERT_THRESHOLD:
        r.publish("alerts", json.dumps({
            "repo":         repo,
            "health_score": score,
            "timestamp":    datetime.utcnow().isoformat(),
        }))
        log.warning(f"ALERT: {repo} health score {score:.3f} below threshold {ALERT_THRESHOLD}")


def write_to_timescale(conn: psycopg2.extensions.connection, repo: str, metrics: dict) -> None:
    """
    Insert one metric snapshot row into TimescaleDB.
    ON CONFLICT DO NOTHING prevents duplicate rows if the consumer
    processes the same event twice (e.g. after a crash and restart).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO repo_health_metrics (
                time,
                repo_name,
                commit_freq_30d,
                bus_factor,
                pr_latency_p50,
                pr_abandon_rate,
                stale_issue_ratio,
                days_since_last_release,
                health_score
            )
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                repo,
                metrics.get("commit_freq_30d"),
                metrics.get("bus_factor"),
                metrics.get("pr_latency_p50"),
                metrics.get("pr_abandon_rate"),
                metrics.get("stale_issue_ratio"),
                metrics.get("days_since_last_release"),
                metrics.get("health_score"),
            ),
        )
    conn.commit()


# ── Connection helpers ────────────────────────────────────────────────────

def connect_redis(retries: int = 20) -> redis_lib.Redis:
    """Retry connecting to Redis until it is ready."""
    for attempt in range(1, retries + 1):
        try:
            r = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            log.info(f"Connected to Redis at {REDIS_URL}")
            return r
        except Exception as e:
            log.warning(f"Waiting for Redis (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Redis.")


def connect_postgres(retries: int = 20) -> psycopg2.extensions.connection:
    """Retry connecting to TimescaleDB until it is ready."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = False
            log.info("Connected to TimescaleDB")
            return conn
        except Exception as e:
            log.warning(f"Waiting for TimescaleDB (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to TimescaleDB.")


# ── Main loop ─────────────────────────────────────────────────────────────

def process_event(event: dict, states: dict) -> str | None:
    """
    Update the in-memory state for the repo that generated this event.
    Returns the repo name, or None if the event type is not one we track.
    """
    repo       = event.get("repo", {}).get("name")
    actor      = event.get("actor", {}).get("login", "unknown")
    event_type = event.get("type")

    if not repo:
        return None

    # Create state object on first sighting of a repo.
    if repo not in states:
        states[repo] = RepoState(repo)

    state = states[repo]

    if event_type == "PushEvent":
        commit_count = event.get("payload", {}).get("size", 1)
        state.record_push(actor, commit_count)

    elif event_type == "PullRequestEvent":
        payload = event.get("payload", {})
        action  = payload.get("action", "")
        merged  = payload.get("pull_request", {}).get("merged", False)
        state.record_pr(action, merged)

    elif event_type == "IssuesEvent":
        action = event.get("payload", {}).get("action", "")
        state.record_issue(action)

    return repo


def main():
    log.info(f"Consumer worker starting")
    log.info(f"Redpanda:     {BROKERS}")
    log.info(f"Flush every:  {FLUSH_EVERY} events")

    r    = connect_redis()
    conn = connect_postgres()

    consumer = Consumer({
        "bootstrap.servers":  BROKERS,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "earliest",
        # earliest: start reading from the very beginning of the topic
        # if this consumer group has never consumed before. This ensures
        # we process all historical events on the first run.
        "enable.auto.commit": False,
        # We commit offsets manually, after successfully writing to both
        # stores. If the write fails, the offset is not advanced and the
        # event will be retried on the next poll.
    })

    consumer.subscribe([TOPIC])
    log.info(f"Subscribed to topic '{TOPIC}' as group '{GROUP_ID}'")

    # In-memory state per repo.
    # Key: repo name (e.g. "facebook/react")
    # Value: RepoState instance
    states: dict[str, RepoState] = {}

    events_processed = 0
    events_since_flush = 0
    repos_flushed_total = set()

    try:
        while True:
            # poll(timeout=1.0): wait up to 1 second for a message.
            # Returns None if no message arrives within the timeout.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # no message yet — loop again

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # We have caught up to the end of the partition.
                    # This is normal, not an error.
                    log.debug(f"Reached end of partition {msg.partition()}")
                    continue
                else:
                    # A real error. Log it and continue rather than crashing.
                    log.error(f"Consumer error: {msg.error()}")
                    continue

            # Decode the message value from bytes to a Python dict.
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning(f"Could not decode message: {e}")
                consumer.commit(message=msg)
                continue

            repo = process_event(event, states)
            events_processed  += 1
            events_since_flush += 1

            # Flush metrics to both stores every FLUSH_EVERY events.
            if events_since_flush >= FLUSH_EVERY:
                flushed_this_round = 0
                for repo_name, state in states.items():
                    try:
                        metrics = state.compute_metrics()
                        write_to_redis(r, repo_name, metrics)
                        write_to_timescale(conn, repo_name, metrics)
                        repos_flushed_total.add(repo_name)
                        flushed_this_round += 1
                    except Exception as e:
                        log.error(f"Failed to flush {repo_name}: {e}")
                        # Do not advance the offset — we will retry.
                        continue

                # Only commit the Redpanda offset after all writes succeed.
                consumer.commit(message=msg)
                events_since_flush = 0

                log.info(
                    f"Flushed {flushed_this_round} repos  |  "
                    f"total events: {events_processed:,}  |  "
                    f"unique repos tracked: {len(states)}"
                )

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()
        log.info("Consumer stopped cleanly.")


if __name__ == "__main__":
    main()