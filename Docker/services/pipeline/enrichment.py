"""Dependency-risk enrichment, on its OWN (slow) cadence.

This is the containerised sibling of the top-level DependencyRisk.py. It runs
daily (not hourly): a repo's supply-chain risk barely moves hour to hour, and
the GitHub SBOM / deps.dev / OSV APIs are rate-limited. Raw API responses land
in bronze; the computed per-repo dimension is written to TimescaleDB (gold).

Keyed on repo_name, exactly like every other layer.
"""

import json
import logging
import re
from urllib.parse import quote

import psycopg2
import requests

import config
import storage

log = logging.getLogger("pipeline.enrichment")

# purl ecosystem -> (deps.dev system, OSV ecosystem)
ECOSYSTEM_MAP = {
    "npm": ("npm", "npm"), "pypi": ("pypi", "PyPI"), "golang": ("go", "Go"),
    "maven": ("maven", "Maven"), "cargo": ("cargo", "crates.io"),
    "nuget": ("nuget", "NuGet"), "gem": ("rubygems", "RubyGems"),
}
OUTDATED_MAJOR_THRESHOLD = 2
_latest_major_cache: dict = {}


def _major(v):
    if not v:
        return None
    m = re.search(r"\d+", v.lstrip("vV="))
    return int(m.group()) if m else None


def _parse_purl(locator):
    m = re.match(r"pkg:(?P<t>[^/]+)/(?P<rest>.+)", locator or "")
    if not m:
        return None
    ptype, rest = m.group("t").lower(), m.group("rest").split("#")[0].split("?")[0]
    version = None
    if "@" in rest:
        rest, version = rest.rsplit("@", 1)
        version = requests.utils.unquote(version)
    parts = [requests.utils.unquote(p) for p in rest.split("/")]
    if ptype == "maven" and len(parts) >= 2:
        name = f"{parts[0]}:{parts[-1]}"
    elif ptype == "golang":
        name = "/".join(parts)
    else:
        name = "/".join(parts) if len(parts) >= 2 else parts[0]
    return ptype, name, version


def _fetch_sbom(repo: str):
    headers = {"Accept": "application/vnd.github+json"}
    if config.GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
    try:
        resp = requests.get(
            f"https://api.github.com/repos/{repo}/dependency-graph/sbom",
            headers=headers, timeout=30,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    # Archive the raw response in bronze for replayability.
    key = f"enrichment/sbom/{repo.replace('/', '__')}.json"
    try:
        storage.put_bytes(config.BRONZE_BUCKET, key, resp.content, "application/json")
    except Exception as e:
        log.warning("enrichment: could not archive sbom for %s: %s", repo, e)

    deps = []
    for pkg in resp.json().get("sbom", {}).get("packages", []):
        for ref in pkg.get("externalRefs", []):
            if ref.get("referenceType") != "purl":
                continue
            parsed = _parse_purl(ref.get("referenceLocator", ""))
            if not parsed or parsed[2] is None:
                continue
            ptype, name, version = parsed
            if ptype in ECOSYSTEM_MAP:
                system, osv_eco = ECOSYSTEM_MAP[ptype]
                deps.append((system, osv_eco, name, version))
    return deps


def _latest_major(system, name):
    key = (system, name)
    if key in _latest_major_cache:
        return _latest_major_cache[key]
    major = None
    try:
        resp = requests.get(
            f"https://api.deps.dev/v3/systems/{system}/packages/{quote(name, safe='')}",
            timeout=30,
        )
        if resp.status_code == 200:
            versions = resp.json().get("versions", [])
            default = [v for v in versions if v.get("isDefault")]
            chosen = default[0] if default else (
                max(versions, key=lambda v: _major(v.get("versionKey", {}).get("version")) or -1)
                if versions else None
            )
            if chosen:
                major = _major(chosen.get("versionKey", {}).get("version"))
    except requests.RequestException:
        pass
    _latest_major_cache[key] = major
    return major


def _osv_advisory_flags(deps):
    queries = [{"package": {"ecosystem": e, "name": n}, "version": v} for (_s, e, n, v) in deps]
    flags = [False] * len(deps)
    if not queries:
        return flags
    try:
        resp = requests.post(
            "https://api.osv.dev/v1/querybatch", json={"queries": queries}, timeout=30,
        )
        if resp.status_code == 200:
            for i, res in enumerate(resp.json().get("results", [])):
                if res.get("vulns"):
                    flags[i] = True
    except requests.RequestException:
        pass
    return flags


def analyse_repo(repo: str) -> dict:
    deps = _fetch_sbom(repo)
    if deps is None:
        return {"repo_name": repo, "available": False,
                "declared_dependency_count": None,
                "outdated_dependency_ratio": None, "open_advisory_count": None}

    comparable = outdated = 0
    for system, _osv, name, version in deps:
        cur, latest = _major(version), _latest_major(system, name)
        if cur is None or latest is None:
            continue
        comparable += 1
        if latest - cur > OUTDATED_MAJOR_THRESHOLD:
            outdated += 1
    ratio = (outdated / comparable) if comparable else None
    advisory_count = sum(_osv_advisory_flags(deps))
    return {"repo_name": repo, "available": True,
            "declared_dependency_count": len(deps),
            "outdated_dependency_ratio": ratio, "open_advisory_count": advisory_count}


def run(repos: list[str]) -> None:
    """Enrich a list of repos and upsert the dimension into TimescaleDB."""
    if not repos:
        log.info("enrichment: no repos to analyse")
        return
    repos = repos[: config.ENRICHMENT_MAX_REPOS]
    log.info("enrichment: analysing %d repos", len(repos))

    rows = [analyse_repo(r) for r in repos]

    conn = psycopg2.connect(config.POSTGRES_DSN)
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO repo_dependency_risk
                    (time, repo_name, declared_dependency_count,
                     outdated_dependency_ratio, open_advisory_count, enrichment_available)
                VALUES (NOW(), %s, %s, %s, %s, %s)
                """,
                (r["repo_name"], r["declared_dependency_count"],
                 r["outdated_dependency_ratio"], r["open_advisory_count"], r["available"]),
            )
    conn.commit()
    conn.close()

    covered = sum(1 for r in rows if r["available"])
    log.info("enrichment: wrote %d rows (%d with data)", len(rows), covered)
