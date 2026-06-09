import json
import time
import random
import logging
import os
from datetime import datetime, UTC

from confluent_kafka import Producer

# ── Logging ─────────────────────────────────────────────────────────────
# basicConfig sets the format for all log messages in this process.
# %(asctime)s: timestamp, %(levelname)s: DEBUG/INFO/ERROR, %(message)s: the text.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration from environment variables ────────────────────────────
# os.getenv("KEY", "default") reads the environment variable KEY.
# If it is not set, it returns the default value.
# These are set in docker-compose.yml under the service's `environment:` block.
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:29092")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
TOPIC = "gh-events"

# ── Fake data ────────────────────────────────────────────────────────────
# These are the repositories the fake data generator will produce events for.
# Mix of actively maintained and some "quiet" repos so health scores vary.

FAKE_REPOS = [
    # High activity repos (will score well)
    "facebook/react",
    "vuejs/vue",
    "torvalds/linux",
    "redis/redis",
    "django/django",
    "fastapi/fastapi",
    "tensorflow/tensorflow",
    # Lower activity repos (will score worse)
    "old-project/abandoned-lib",
    "single-dev/solo-tool",
    "legacy-corp/old-sdk",
    "unmaintained/helper-utils",
    "dormant-org/stale-framework",
]

# These names are used as commit authors.
# A project where most commits come from one person has a low bus factor.
REPO_AUTHOR_PROFILES = {
    # Healthy: many contributors
    "facebook/react":            ["gaearon", "sebmarkbage", "zpao", "acdlite", "lunaruan"],
    "torvalds/linux":            ["torvalds", "gregkh", "tglx", "axboe", "mingo"],
    "tensorflow/tensorflow":     ["tensorflower", "mrry", "yongtang", "rmlarsen", "mihaimaruseac"],
    # At-risk: single maintainer
    "old-project/abandoned-lib": ["original-author"],
    "single-dev/solo-tool":      ["solo-dev"],
    "unmaintained/helper-utils": ["old-maintainer"],
    "dormant-org/stale-framework": ["dormant-dev"],
}

DEFAULT_AUTHORS = ["alice", "bob", "charlie", "diana", "eve"]

EVENT_WEIGHTS = {
    # Event type: relative probability weight
    "PushEvent":        50,   # most common — every commit is a push
    "PullRequestEvent": 20,
    "IssuesEvent":      20,
    "WatchEvent":        7,   # someone starring the repo
    "ForkEvent":         3,
}


def get_authors(repo: str) -> list:
    """Return the author pool for a repo, or a default pool."""
    return REPO_AUTHOR_PROFILES.get(repo, DEFAULT_AUTHORS)


def make_push_event(repo: str, actor: str) -> dict:
    """Simulate a PushEvent (one or more commits pushed to a branch)."""
    return {
        "id":    str(random.randint(10**9, 10**10)),
        "type":  "PushEvent",
        "actor": {"login": actor},
        "repo":  {"name": repo},
        "payload": {
            "ref":  "refs/heads/main",
            "size": random.randint(1, 6),   # number of commits in this push
            "commits": [
                {
                    "sha":    f"{random.randint(0, 0xFFFFFF):06x}",
                    "author": {"name": actor, "email": f"{actor}@example.com"},
                    "message": random.choice([
                        "fix: resolve edge case in parser",
                        "feat: add new configuration option",
                        "docs: update README",
                        "refactor: simplify authentication flow",
                        "chore: bump dependency versions",
                        "test: add coverage for edge cases",
                    ]),
                }
            ],
        },
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def make_pr_event(repo: str, actor: str) -> dict:
    """Simulate a PullRequestEvent (opened, closed, or merged)."""
    action = random.choices(
        ["opened", "closed", "reopened"],
        weights=[60, 35, 5]
    )[0]
    merged = action == "closed" and random.random() > 0.25
    return {
        "id":    str(random.randint(10**9, 10**10)),
        "type":  "PullRequestEvent",
        "actor": {"login": actor},
        "repo":  {"name": repo},
        "payload": {
            "action": action,
            "number": random.randint(1, 5000),
            "pull_request": {
                "state":      "open" if action == "opened" else "closed",
                "merged":     merged,
                "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
        },
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def make_issue_event(repo: str, actor: str) -> dict:
    """Simulate an IssuesEvent (opened or closed)."""
    action = random.choices(["opened", "closed"], weights=[60, 40])[0]
    return {
        "id":    str(random.randint(10**9, 10**10)),
        "type":  "IssuesEvent",
        "actor": {"login": actor},
        "repo":  {"name": repo},
        "payload": {
            "action": action,
            "issue":  {
                "number":     random.randint(1, 10000),
                "state":      "open" if action == "opened" else "closed",
                "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
        },
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def make_generic_event(event_type: str, repo: str, actor: str) -> dict:
    """Simulate a WatchEvent or ForkEvent."""
    return {
        "id":      str(random.randint(10**9, 10**10)),
        "type":    event_type,
        "actor":   {"login": actor},
        "repo":    {"name": repo},
        "payload": {},
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def generate_event() -> tuple[str, dict]:
    """
    Pick a random repo and event type, return (repo_name, event_dict).
    Repos with fewer configured authors generate events less frequently
    to simulate lower-activity projects.
    """
    # Weight repo selection: high-activity repos appear more often
    weights = [
        5 if repo in REPO_AUTHOR_PROFILES and len(REPO_AUTHOR_PROFILES[repo]) > 2
        else 1
        for repo in FAKE_REPOS
    ]
    repo  = random.choices(FAKE_REPOS, weights=weights)[0]
    actor = random.choice(get_authors(repo))

    event_type = random.choices(
        list(EVENT_WEIGHTS.keys()),
        weights=list(EVENT_WEIGHTS.values()),
    )[0]

    if event_type == "PushEvent":
        return repo, make_push_event(repo, actor)
    elif event_type == "PullRequestEvent":
        return repo, make_pr_event(repo, actor)
    elif event_type == "IssuesEvent":
        return repo, make_issue_event(repo, actor)
    else:
        return repo, make_generic_event(event_type, repo, actor)


def delivery_report(err, msg):
    """
    Callback invoked by the Kafka producer for every message after it
    is acknowledged (or fails). We only log errors here.
    """
    if err is not None:
        log.error(f"Message delivery failed: {err}")


def wait_for_broker(producer: Producer, retries: int = 20) -> None:
    """
    Block until Redpanda is ready to accept messages.
    Even though depends_on: service_healthy handles most of this,
    there is a small window where the broker is healthy but not
    yet ready to accept producer connections.
    """
    for attempt in range(1, retries + 1):
        try:
            metadata = producer.list_topics(timeout=5)
            log.info(f"Connected to Redpanda. Topics: {list(metadata.topics.keys())}")
            return
        except Exception as e:
            log.warning(f"Waiting for Redpanda (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Redpanda after multiple retries.")


def main():
    mode = "FAKE DATA" if not GITHUB_TOKEN else "LIVE (GitHub API)"
    log.info(f"Ingestion worker starting in {mode} mode")
    log.info(f"Connecting to Redpanda at {BROKERS}")

    # Producer configuration.
    # linger.ms and batch.size control batching:
    #   linger.ms=5 means wait up to 5ms before sending a batch,
    #   allowing more messages to be bundled together.
    # This reduces the number of network round-trips at the cost of
    # a small latency increase — fine for our use case.
    producer = Producer({
        "bootstrap.servers":     BROKERS,
        "client.id":             "oss-ingestion-worker",
        "linger.ms":             5,
        "batch.size":            65536,
        "compression.type":      "lz4",
        "acks":                  "all",
        # acks=all means Redpanda only acknowledges a message once it
        # has been written to disk. Stronger durability guarantee.
    })

    wait_for_broker(producer)

    log.info("Starting fake event generation. Press Ctrl+C to stop.")
    log.info(f"Tracking {len(FAKE_REPOS)} repositories")

    produced   = 0
    start_time = time.time()

    try:
        while True:
            repo, event = generate_event()

            producer.produce(
                topic    = TOPIC,
                key      = repo.encode("utf-8"),
                # The key determines which Redpanda partition this message
                # goes to. Using repo_name as the key ensures all events
                # for a given repo land in the same partition, preserving
                # per-repo ordering.
                value    = json.dumps(event).encode("utf-8"),
                callback = delivery_report,
            )

            # poll(0) triggers delivery callbacks (like our delivery_report above)
            # without blocking. You must call poll regularly or the callback
            # queue grows unboundedly.
            producer.poll(0)

            produced += 1

            if produced % 500 == 0:
                elapsed = time.time() - start_time
                rate    = produced / elapsed
                producer.flush()   # ensure all buffered messages are sent
                log.info(
                    f"Produced {produced:,} events  |  "
                    f"{rate:.0f} events/sec  |  "
                    f"topic: {TOPIC}"
                )

            time.sleep(0.05)   # ~20 events/second

    except KeyboardInterrupt:
        log.info("Shutting down — flushing remaining messages...")
        producer.flush()
        log.info(f"Done. Total events produced: {produced:,}")


if __name__ == "__main__":
    main()