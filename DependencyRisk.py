"""Dependency-risk enrichment for the GH Archive Parquet dataset.

WHY A SEPARATE SCRIPT
---------------------
GH Archive is an *event stream*: it never carries a repository's dependency
manifests or lockfiles, so `outdated_dependency_ratio` and
`security_advisory_count` cannot be derived from the Parquet alone. They have to
be computed by enriching each repository with external data. The only thing we
need from the Parquet is the join key we already store: `repo_name` ("owner/repo").

METHOD (all free APIs, no paid services)
----------------------------------------
For every distinct repo seen in the Parquet:

  1. DECLARED DEPENDENCIES  -> GitHub Dependency-Graph SBOM API
       GET /repos/{owner}/{repo}/dependency-graph/sbom
     Returns an SPDX document whose packages carry Package URLs (purls) such as
     "pkg:npm/lodash@4.17.21". We parse each purl into (ecosystem, name, version).
     (Requires a GitHub token in $GITHUB_TOKEN; a free read-only PAT is enough.)

  2. OUTDATED RATIO         -> deps.dev API  (https://api.deps.dev, no auth)
       GET /v3/systems/{system}/packages/{name}
     Gives every published version; we take the default/latest stable version and
     compare major numbers. A dependency is "outdated" when it is >2 MAJOR
     versions behind latest. outdated_dependency_ratio = outdated / total.

  3. SECURITY ADVISORIES    -> OSV.dev API   (https://api.osv.dev, no auth)
       POST /v1/querybatch  with all (ecosystem, name, version) pairs at once.
     security_advisory_count = number of declared deps with >=1 open advisory.

Output: ./processed_parquet/dependency_risk.parquet  (one row per repo).

This is intentionally best-effort and side-car: failures for one repo/package
never abort the run, and results are keyed on `repo_name` so they join straight
back onto the event metrics.
"""

import os
import re
import duckdb
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
PARQUET_DIR = "./processed_parquet"
PARQUET_GLOB = os.path.join(PARQUET_DIR, "gh_events_*.parquet")
OUTPUT_PATH = os.path.join(PARQUET_DIR, "dependency_risk.parquet")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
MAX_REPOS = int(os.environ.get("MAX_REPOS", "200"))   # cap API load; 0 = no cap
MAX_WORKERS = 8                                        # concurrent repos
OUTDATED_MAJOR_THRESHOLD = 2                           # >2 majors behind = outdated
REQUEST_TIMEOUT = 30

# purl ecosystem (left) -> (deps.dev system, OSV ecosystem) (right)
ECOSYSTEM_MAP = {
    "npm":    ("npm", "npm"),
    "pypi":   ("pypi", "PyPI"),
    "golang": ("go", "Go"),
    "maven":  ("maven", "Maven"),
    "cargo":  ("cargo", "crates.io"),
    "nuget":  ("nuget", "NuGet"),
    "gem":    ("rubygems", "RubyGems"),
}

_session = requests.Session()
_latest_version_cache = {}   # (system, name) -> latest major int (or None)


def load_repos(limit):
    """Return distinct 'owner/repo' names present in the event Parquet."""
    con = duckdb.connect()
    q = f"""
        SELECT repo_name, COUNT(*) AS events
        FROM read_parquet('{PARQUET_GLOB}')
        WHERE repo_name IS NOT NULL
        GROUP BY repo_name
        ORDER BY events DESC
    """
    if limit and limit > 0:
        q += f" LIMIT {limit}"
    return [r[0] for r in con.execute(q).fetchall()]


def parse_purl(locator):
    """Parse a Package URL into (purl_type, name, version) or None.

    Handles namespaces: maven 'group/artifact' -> 'group:artifact',
    golang keeps the full module path, npm scopes ('%40scope/name') are restored.
    """
    m = re.match(r"pkg:(?P<type>[^/]+)/(?P<rest>.+)", locator)
    if not m:
        return None
    ptype = m.group("type").lower()
    rest = m.group("rest")

    # Strip qualifiers (?...) and subpath (#...)
    rest = rest.split("#", 1)[0].split("?", 1)[0]

    version = None
    if "@" in rest:
        rest, version = rest.rsplit("@", 1)
        version = requests.utils.unquote(version)

    parts = [requests.utils.unquote(p) for p in rest.split("/")]
    if ptype == "maven" and len(parts) >= 2:
        name = f"{parts[0]}:{parts[-1]}"
    elif ptype == "golang":
        name = "/".join(parts)
    elif len(parts) >= 2:           # scoped npm etc.: namespace + name
        name = "/".join(parts)
    else:
        name = parts[0]
    return ptype, name, version


def major_of(version):
    """Best-effort major-version integer from a version string, else None."""
    if not version:
        return None
    m = re.search(r"\d+", version.lstrip("vV="))
    return int(m.group()) if m else None


def depsdev_latest_major(system, name):
    """Latest stable major version for a package via deps.dev (cached)."""
    key = (system, name)
    if key in _latest_version_cache:
        return _latest_version_cache[key]

    major = None
    try:
        url = f"https://api.deps.dev/v3/systems/{system}/packages/{quote(name, safe='')}"
        resp = _session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            versions = resp.json().get("versions", [])
            default = [v for v in versions if v.get("isDefault")]
            chosen = default[0] if default else None
            if chosen is None and versions:
                # Fall back to the highest parseable major.
                chosen = max(
                    versions,
                    key=lambda v: major_of(v.get("versionKey", {}).get("version")) or -1,
                )
            if chosen:
                major = major_of(chosen.get("versionKey", {}).get("version"))
    except requests.RequestException:
        pass

    _latest_version_cache[key] = major
    return major


def osv_advisory_flags(deps):
    """Return a list[bool] (aligned with deps) marking which have >=1 advisory.

    deps: list of (osv_ecosystem, name, version). One batched OSV.dev call.
    """
    queries = [
        {"package": {"ecosystem": eco, "name": name}, "version": ver}
        for (eco, name, ver) in deps
    ]
    flags = [False] * len(deps)
    if not queries:
        return flags
    try:
        resp = _session.post(
            "https://api.osv.dev/v1/querybatch",
            json={"queries": queries},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            for i, result in enumerate(resp.json().get("results", [])):
                if result.get("vulns"):
                    flags[i] = True
    except requests.RequestException:
        pass
    return flags


def fetch_dependencies(repo):
    """Fetch declared dependencies for 'owner/repo' from the GitHub SBOM API.

    Returns list of (purl_type, deps.dev system, OSV ecosystem, name, version).
    """
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    url = f"https://api.github.com/repos/{repo}/dependency-graph/sbom"

    try:
        resp = _session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None

    out = []
    packages = resp.json().get("sbom", {}).get("packages", [])
    for pkg in packages:
        for ref in pkg.get("externalRefs", []):
            if ref.get("referenceType") != "purl":
                continue
            parsed = parse_purl(ref.get("referenceLocator", ""))
            if not parsed:
                continue
            ptype, name, version = parsed
            mapping = ECOSYSTEM_MAP.get(ptype)
            if not mapping or version is None:
                continue
            system, osv_eco = mapping
            out.append((ptype, system, osv_eco, name, version))
    return out


def analyse_repo(repo):
    """Compute dependency-risk metrics for a single repository."""
    deps = fetch_dependencies(repo)
    if deps is None:
        return {"repo_name": repo, "dependency_data_available": False,
                "declared_dependency_count": None,
                "outdated_dependency_ratio": None,
                "security_advisory_count": None}

    total = len(deps)
    # --- outdated ratio (deps.dev) ---
    comparable = 0
    outdated = 0
    for _ptype, system, _osv, name, version in deps:
        cur_major = major_of(version)
        latest_major = depsdev_latest_major(system, name)
        if cur_major is None or latest_major is None:
            continue
        comparable += 1
        if latest_major - cur_major > OUTDATED_MAJOR_THRESHOLD:
            outdated += 1
    outdated_ratio = (outdated / comparable) if comparable else None

    # --- advisory count (OSV.dev) ---
    osv_input = [(osv, name, version) for (_p, _s, osv, name, version) in deps]
    flags = osv_advisory_flags(osv_input)
    advisory_count = sum(flags)

    return {"repo_name": repo, "dependency_data_available": True,
            "declared_dependency_count": total,
            "outdated_dependency_ratio": outdated_ratio,
            "security_advisory_count": advisory_count}


def main():
    print("--- Dependency-Risk Enrichment ---")
    if not GITHUB_TOKEN:
        print("[!] $GITHUB_TOKEN is not set. The GitHub SBOM API requires a token "
              "(a free read-only PAT works). Without it every repo returns no data.")

    repos = load_repos(MAX_REPOS)
    print(f"  {len(repos)} repositories to analyse "
          f"(cap MAX_REPOS={MAX_REPOS or 'none'}).")

    rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyse_repo, r): r for r in repos}
        done = 0
        for future in as_completed(futures):
            done += 1
            try:
                rows.append(future.result())
            except Exception as e:
                rows.append({"repo_name": futures[future],
                             "dependency_data_available": False,
                             "declared_dependency_count": None,
                             "outdated_dependency_ratio": None,
                             "security_advisory_count": None})
                print(f"  [!] {futures[future]} failed: {e}")
            if done % 25 == 0 or done == len(repos):
                print(f"    Progress: {done}/{len(repos)} repos.")

    write_parquet(rows)
    covered = sum(1 for r in rows if r["dependency_data_available"])
    print(f"  Wrote {len(rows)} rows ({covered} with dependency data) to {OUTPUT_PATH}.")
    print("--- Done. Join back on repo_name. ---")


def write_parquet(rows):
    """Persist result rows to Parquet via an explicit DuckDB table (no pandas)."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE risk (
            repo_name VARCHAR,
            dependency_data_available BOOLEAN,
            declared_dependency_count INTEGER,
            outdated_dependency_ratio DOUBLE,
            security_advisory_count INTEGER
        );
    """)
    con.executemany(
        "INSERT INTO risk VALUES (?, ?, ?, ?, ?);",
        [(r["repo_name"], r["dependency_data_available"],
          r["declared_dependency_count"], r["outdated_dependency_ratio"],
          r["security_advisory_count"]) for r in rows],
    )
    con.execute(
        f"COPY risk TO '{OUTPUT_PATH}' (FORMAT PARQUET, COMPRESSION 'ZSTD');"
    )


if __name__ == "__main__":
    main()
