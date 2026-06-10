import csv

METRICS_PATH = "data/repository_health_metrics_jan2023.csv"
TREND_PATH = "data/activity_trend_v2_jan2023.csv"
OUTPUT_PATH = "data/health_score_v0_jan2023.csv"


def clamp(value, min_value=0, max_value=100):
    return max(min_value, min(value, max_value))


def contributor_score(active_contributors, top_contributor_share):
    active_score = clamp((active_contributors / 1000) * 100)
    concentration_score = clamp((1 - top_contributor_share) * 100)
    return 0.5 * active_score + 0.5 * concentration_score


def activity_score(activity_trend):
    if activity_trend is None:
        return 50

    # 0% trend -> 80 points
    # +20% or more -> 100 points
    # -50% or worse -> 0 points
    score = 80 + (activity_trend * 100)
    return clamp(score)


def maintenance_score(maintenance_gap_days):
    if maintenance_gap_days <= 1:
        return 100
    if maintenance_gap_days <= 7:
        return 80
    if maintenance_gap_days <= 30:
        return 40
    return 0


def bot_score(bot_ratio):
    return clamp((1 - bot_ratio) * 100)


def pr_issue_score(pr_events, issue_events, total_events):
    if total_events == 0:
        return 0

    interaction_ratio = (pr_events + issue_events) / total_events

    # If at least 20% of events are PR/issue-related, give full score.
    return clamp((interaction_ratio / 0.20) * 100)


def risk_level(score):
    if score >= 75:
        return "LOW"
    if score >= 50:
        return "MEDIUM"
    return "HIGH"


def read_csv_by_repo(path):
    data = {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            data[row["repository"]] = row

    return data


def main():
    metrics = read_csv_by_repo(METRICS_PATH)
    trends = read_csv_by_repo(TREND_PATH)

    rows = []

    for repo, m in metrics.items():
        trend_row = trends.get(repo)

        active_contributors = int(m["active_contributors"])
        top_contributor_share = float(m["top_contributor_share"])
        bot_ratio = float(m["bot_ratio"])
        maintenance_gap_days = int(m["maintenance_gap_days"])
        pr_events = int(m["pr_events"])
        issue_events = int(m["issue_events"]) + int(m["issue_comment_events"])
        total_events = int(m["total_events"])

        activity_trend = None
        if trend_row is not None and trend_row["activity_trend"] != "":
            activity_trend = float(trend_row["activity_trend"])

        c_score = contributor_score(active_contributors, top_contributor_share)
        a_score = activity_score(activity_trend)
        m_score = maintenance_score(maintenance_gap_days)
        b_score = bot_score(bot_ratio)
        pi_score = pr_issue_score(pr_events, issue_events, total_events)

        health_score = (
            0.30 * c_score
            + 0.25 * a_score
            + 0.20 * m_score
            + 0.15 * b_score
            + 0.10 * pi_score
        )

        rows.append({
            "repository": repo,
            "health_score": round(health_score, 2),
            "risk_level": risk_level(health_score),
            "contributor_score": round(c_score, 2),
            "activity_score": round(a_score, 2),
            "maintenance_score": round(m_score, 2),
            "bot_score": round(b_score, 2),
            "pr_issue_score": round(pi_score, 2),
            "active_contributors": active_contributors,
            "top_contributor_share": round(top_contributor_share, 4),
            "bot_ratio": round(bot_ratio, 4),
            "activity_trend": round(activity_trend, 4) if activity_trend is not None else "",
            "maintenance_gap_days": maintenance_gap_days,
        })

    rows = sorted(rows, key=lambda x: x["health_score"], reverse=True)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        fieldnames = rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("=== Health Score V0 ===")

    for row in rows:
        print(row)

    print(f"\nSaved output to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()