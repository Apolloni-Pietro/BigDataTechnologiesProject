"""ML 'risk factor' for a repository.

The risk factor is **unsupervised**: we have no labelled ground truth of which
repos later became unhealthy, so we cannot train a classifier honestly. Instead
we learn the *normal* distribution of repo feature vectors with an
IsolationForest and treat statistical outliers (plus a directional adjustment)
as higher risk. When there are too few repos to train, we fall back to a
transparent deterministic composite so the pipeline always produces a number.

Output: risk_score in [0, 1], where 1 = highest risk.
"""

import logging
import pickle

import numpy as np

import config
import storage

log = logging.getLogger("pipeline.risk")

MODEL_KEY = "models/risk_isolation_forest.pkl"

# Features fed to the model, in a fixed order. All are "higher = riskier" after
# the transforms applied in gold.py, except where noted in _composite().
FEATURE_COLUMNS = [
    "commit_freq_30d",        # lower  = riskier
    "active_contributors_90d",# lower  = riskier
    "bus_factor",             # lower  = riskier
    "pr_abandon_rate",        # higher = riskier
    "stale_issue_ratio",      # higher = riskier
    "days_since_last_commit", # higher = riskier
]

MIN_ROWS_TO_TRAIN = 30


def _composite(feats: dict) -> float:
    """Deterministic fallback risk score in [0, 1] (used when ML can't train)."""
    def norm(v, lo, hi):
        if v is None:
            return 0.5
        return float(min(1.0, max(0.0, (v - lo) / (hi - lo))))

    # Each term is oriented so that 1.0 = risky. Weights sum to 1.0 (the two
    # supply-chain terms were removed with the enrichment path; the remaining six
    # were rescaled to keep the score in [0, 1]).
    activity_risk   = 1.0 - norm(feats.get("commit_freq_30d"), 0, 5)
    contributor_risk= 1.0 - norm(feats.get("active_contributors_90d"), 1, 8)
    bus_risk        = 1.0 - norm(feats.get("bus_factor"), 1, 5)
    abandon_risk    = norm(feats.get("pr_abandon_rate"), 0, 1)
    stale_risk      = norm(feats.get("stale_issue_ratio"), 0, 1)
    freshness_risk  = norm(feats.get("days_since_last_commit"), 0, 180)

    return round(
        activity_risk    * 0.22 +
        contributor_risk * 0.17 +
        bus_risk         * 0.17 +
        abandon_risk     * 0.17 +
        stale_risk       * 0.10 +
        freshness_risk   * 0.17,
        4,
    )


def _matrix(rows: list[dict]) -> np.ndarray:
    """Build a feature matrix; missing values imputed with the column median."""
    raw = np.array(
        [[(r.get(c) if r.get(c) is not None else np.nan) for c in FEATURE_COLUMNS] for r in rows],
        dtype=float,
    )
    col_median = np.nanmedian(raw, axis=0)
    col_median = np.where(np.isnan(col_median), 0.0, col_median)
    inds = np.where(np.isnan(raw))
    raw[inds] = np.take(col_median, inds[1])
    return raw


def score_repos(rows: list[dict]) -> dict[str, float]:
    """Return {repo_name: risk_score} for a batch of per-repo feature dicts.

    Trains a fresh IsolationForest on the batch (and persists it to MinIO for
    inspection/reuse). Falls back to the deterministic composite if the batch is
    too small to learn a meaningful distribution.
    """
    repos = [r["repo_name"] for r in rows]

    if len(rows) < MIN_ROWS_TO_TRAIN:
        log.info("risk: only %d repos (<%d) — using deterministic composite",
                 len(rows), MIN_ROWS_TO_TRAIN)
        return {r["repo_name"]: _composite(r) for r in rows}

    try:
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler

        X = StandardScaler().fit_transform(_matrix(rows))
        model = IsolationForest(
            n_estimators=200, contamination="auto", random_state=42, n_jobs=-1,
        )
        model.fit(X)

        # decision_function: higher = more normal. Map to risk in [0,1] where
        # 1 = most anomalous, then blend with the composite so the score stays
        # interpretable and directionally correct (anomalous-but-thriving repos
        # shouldn't read as 'risky').
        anomaly = -model.decision_function(X)
        lo, hi = anomaly.min(), anomaly.max()
        anomaly_norm = (anomaly - lo) / (hi - lo) if hi > lo else np.zeros_like(anomaly)

        scores = {}
        for i, r in enumerate(rows):
            blended = 0.6 * _composite(r) + 0.4 * float(anomaly_norm[i])
            scores[r["repo_name"]] = round(blended, 4)

        try:
            storage.put_bytes(config.GOLD_BUCKET, MODEL_KEY, pickle.dumps(model))
        except Exception as e:  # persistence is best-effort
            log.warning("risk: could not persist model: %s", e)

        log.info("risk: scored %d repos with IsolationForest", len(rows))
        return scores

    except Exception as e:
        log.warning("risk: ML path failed (%s) — falling back to composite", e)
        return {r["repo_name"]: _composite(r) for r in rows}
