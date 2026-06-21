"""The pipeline's single source of "now".

In replay mode (config.REPLAY_OFFSET_YEARS > 0) every "what time is it?" question
is answered by a clock shifted N years into the past, so the pipeline fetches and
processes the feed from N years ago while the data keeps its true dates. Exactly
three places consult this clock — the scheduler (which hour to fetch), gold (its
rolling-window + recency math) and retention (its prune cutoff) — and they must
all agree, or e.g. retention would treat year-old replay data as expired.

This module imports only config (no other pipeline modules) so gold/retention/
pipeline can all import it without creating cycles.
"""

from datetime import datetime, timedelta, timezone

import config


def _offset() -> timedelta:
    # Calendar years approximated as 365 days each — stdlib only, and exact enough
    # for "one year ago" (the replay just needs the right era, not leap-second precision).
    return timedelta(days=365 * config.REPLAY_OFFSET_YEARS)


def effective_now() -> datetime:
    """The pipeline's logical 'now' (real now, shifted back REPLAY_OFFSET_YEARS)."""
    return datetime.now(timezone.utc) - _offset()


def effective_today() -> "datetime.date":
    """The logical current date (for gold's `current_date`-style comparisons)."""
    return effective_now().date()


def effective_latest_hour(lag_hours: int) -> datetime:
    """The most recent whole hour the (shifted) feed should have published."""
    now = effective_now().replace(minute=0, second=0, microsecond=0)
    return now - timedelta(hours=lag_hours)
