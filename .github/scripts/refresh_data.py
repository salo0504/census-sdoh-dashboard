#!/usr/bin/env python3
"""
refresh_data.py — standalone ACS SDOH data refresh for GitHub Actions.

No MCP framework and no web server: this is a plain script that uses only
httpx + the Python standard library, so it runs in a minimal CI job.
It mirrors the indicator definitions and pull/validate logic of server.py.

RUN MODES (selected via environment variables):
  PULL_ALL="true"          Full rebuild from START_YEAR to the latest available
                           year. Use once to initialise, or to re-pull everything
                           after a definition fix.
  SPECIFIC_YEAR="2025"     Pull just that one year and merge it in.
  (neither set)            Default: pull the latest available ACS year and append
                           it. If the latest year is already present, or not yet
                           released by the Census Bureau, exit cleanly (no commit).

REQUIRED ENV: CENSUS_API_KEY, GITHUB_TOKEN, GITHUB_REPO
OPTIONAL ENV: DATA_FILE_PATH (default: data/data_all_years.json)
              GITHUB_BRANCH  (default: main)
              START_YEAR     (default: 2005)

FAIL-CLOSED SAFETY:
  Before committing, the script runs a year-over-year consistency check. If any
  indicator's typical value shifts implausibly against the adjacent year (the
  fingerprint of a Census table redesign, exactly what once corrupted the
  uninsured / broadband / single-parent series), the script COMMITS NOTHING and
  exits non-zero, so the GitHub Action fails and a human can review. It never
  silently publishes suspect data.

EXIT CODES:
  0  success (committed) OR nothing to do (latest already present / not released)
  1  a real error, or a consistency check tripped (nothing committed)
"""

import os
import sys
import json
import base64
import time
import statistics
from datetime import datetime, timezone

import httpx

# ─────────────────────────── configuration ───────────────────────────────────

def _require(name):
    v = os.environ.get(name)
    if not v:
        sys.stderr.write(
            "FATAL: missing required environment variable '%s'. "
            "Set it as a repository secret / workflow env.\n" % name
        )
        sys.exit(1)
    return v

CENSUS_API_KEY = _require("CENSUS_API_KEY")
GITHUB_TOKEN   = _require("GITHUB_TOKEN")
GITHUB_REPO    = _require("GITHUB_REPO")
DATA_FILE_PATH = os.environ.get("DATA_FILE_PATH", "data/data_all_years.json")
GITHUB_BRANCH  = os.environ.get("GITHUB_BRANCH", "main")
START_YEAR     = int(os.environ.get("START_YEAR", "2005"))
PULL_ALL       = os.environ.get("PULL_ALL", "false").strip().lower() in ("1", "true", "yes")
SPECIFIC_YEAR  = os.environ.get("SPECIFIC_YEAR", "").strip()

# Year-over-year shift thresholds. A median ratio outside this band between
# adjacent years signals a likely table/variable redesign rather than real change.
YOY_HI = 1.5
YOY_LO = 0.67

CENSUS_BASE = "https://api.census.gov/data"
SUPPRESSION = {"-666666666", "-999999999", "-888888888", "N", "NA", "(X)", "null", "None"}

# ─────────────────────────── indicator definitions ───────────────────────────
# Kept in lock-step with server.py. The available_from / available_to windows are
# the load-bearing correctness fix: they keep Census table redesigns out of the
# published series (uninsured 2015+, broadband 2016+, single-parent 2005–2014,
# and the subject-table indicators 2010+).

INDICATORS = {
    "median_household_income": {
        "label": "Median Household Income", "category": "Economic Stability",
        "table": "B19013", "variables": ["B19013_001E"], "formula": "single",
        "unit": "dollars", "series": "acs1",
        "description": "Median household income in the past 12 months (inflation-adjusted dollars)",
        "higher_is": "better", "available_from": 2005,
    },
    "poverty_rate": {
        "label": "Poverty Rate", "category": "Economic Stability",
        "table": "S1701", "variables": ["S1701_C03_001E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Percent of people below poverty level",
        "higher_is": "worse", "available_from": 2010,
    },
    "employment_rate": {
        "label": "Employment Rate", "category": "Economic Stability",
        "table": "S2301", "variables": ["S2301_C03_001E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Employment-population ratio, civilian population 16+",
        "higher_is": "better", "available_from": 2010,
    },
    "snap_receipt_rate": {
        "label": "SNAP Receipt Rate", "category": "Economic Stability",
        "table": "B22003", "variables": ["B22003_002E", "B22003_001E"], "formula": "percent_of_total",
        "unit": "percent", "series": "acs1",
        "description": "Percent of households receiving SNAP (food stamps)",
        "higher_is": "worse", "available_from": 2005,
    },
    "hs_diploma_rate": {
        "label": "High School Diploma or Higher", "category": "Education",
        "table": "S1501", "variables": ["S1501_C02_014E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Percent of population 25+ with HS diploma or higher",
        "higher_is": "better", "available_from": 2010,
    },
    "bachelors_rate": {
        "label": "Bachelor's Degree or Higher", "category": "Education",
        "table": "S1501", "variables": ["S1501_C02_015E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Percent of population 25+ with bachelor's degree or higher",
        "higher_is": "better", "available_from": 2010,
    },
    "uninsured_rate": {
        "label": "Uninsured Rate (Under 65)", "category": "Healthcare Access",
        "table": "S2701", "variables": ["S2701_C05_001E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Percent of civilian noninstitutionalized population under 65 without health insurance",
        # S2701 redesigned 2015; C05 meant an insured/coverage figure before then.
        "higher_is": "worse", "available_from": 2015,
    },
    "disability_rate": {
        "label": "Disability Rate", "category": "Healthcare Access",
        "table": "S1810", "variables": ["S1810_C03_001E"], "formula": "single",
        "unit": "percent", "series": "acs1/subject",
        "description": "Percent of civilian noninstitutionalized population with a disability",
        "higher_is": "neutral", "available_from": 2010,
    },
    "median_gross_rent": {
        "label": "Median Gross Rent", "category": "Housing",
        "table": "B25064", "variables": ["B25064_001E"], "formula": "single",
        "unit": "dollars", "series": "acs1",
        "description": "Median gross rent (dollars per month)",
        "higher_is": "neutral", "available_from": 2005,
    },
    "homeownership_rate": {
        "label": "Homeownership Rate", "category": "Housing",
        "table": "B25003", "variables": ["B25003_002E", "B25003_001E"], "formula": "percent_of_total",
        "unit": "percent", "series": "acs1",
        "description": "Percent of occupied housing units that are owner-occupied",
        "higher_is": "better", "available_from": 2005,
    },
    "housing_cost_burden": {
        "label": "Housing Cost Burden (Renters)", "category": "Housing",
        "table": "B25070",
        "variables": ["B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E", "B25070_001E"],
        "formula": "sum_over_total", "unit": "percent", "series": "acs1",
        "description": "Percent of renters paying 30% or more of income on housing",
        "higher_is": "worse", "available_from": 2005,
    },
    "foreign_born_pct": {
        "label": "Foreign-Born Population", "category": "Community Context",
        "table": "B05002", "variables": ["B05002_013E", "B05002_001E"], "formula": "percent_of_total",
        "unit": "percent", "series": "acs1",
        "description": "Percent of population that is foreign-born",
        "higher_is": "neutral", "available_from": 2005,
    },
    "non_english_household_pct": {
        "label": "Non-English Speaking Households", "category": "Community Context",
        "table": "B16010", "variables": ["B16010_002E", "B16010_001E"], "formula": "percent_of_total",
        "unit": "percent", "series": "acs1",
        "description": "Percent of households where a language other than English is spoken",
        "higher_is": "neutral", "available_from": 2010,
    },
    "single_parent_pct": {
        "label": "Single-Parent Households", "category": "Community Context",
        "table": "B11012", "variables": ["B11012_010E", "B11012_015E", "B11012_001E"],
        "formula": "sum_over_total", "unit": "percent", "series": "acs1",
        "description": "Percent of family households with a single parent",
        # B11012 restructured ~2019; codes only match the family-household basis
        # through 2014. TODO(owner): re-map for 2019+ and lift available_to.
        "higher_is": "neutral", "available_from": 2005, "available_to": 2014,
    },
    "broadband_access": {
        "label": "Broadband Internet Access", "category": "Broadband Access",
        "table": "B28002", "variables": ["B28002_004E", "B28002_001E"], "formula": "percent_of_total",
        "unit": "percent", "series": "acs1",
        "description": "Percent of households with a broadband internet subscription",
        # B28002 redesigned 2016; pre-2016 _004E was a narrow subtype (~15%).
        "higher_is": "better", "available_from": 2016,
    },
}

# ─────────────────────────── pull + compute ──────────────────────────────────

def safe_val(v):
    s = str(v)
    if s in SUPPRESSION or (s.startswith("-") and len(s) > 1):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def compute(formula, variables, row):
    if formula == "single":
        return safe_val(row.get(variables[0]))
    if formula == "percent_of_total":
        n, d = safe_val(row.get(variables[0])), safe_val(row.get(variables[1]))
        return round(n / d * 100, 2) if n is not None and d else None
    if formula == "sum_over_total":
        parts = [safe_val(row.get(v)) for v in variables[:-1]]
        d = safe_val(row.get(variables[-1]))
        return round(sum(parts) / d * 100, 2) if all(p is not None for p in parts) and d else None
    return None

def census_get(client, series, year, variables):
    url = "%s/%d/acs/%s" % (CENSUS_BASE, year, series)
    params = {"get": "NAME," + ",".join(variables), "for": "state:*", "key": CENSUS_API_KEY}
    r = client.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def pull_one(client, defn, year):
    """Returns {'states': {...}} or {'states': {}, 'error'|'note': ...}."""
    if year < defn.get("available_from", 2005):
        return {"states": {}, "note": "before available_from"}
    if defn.get("available_to") is not None and year > defn["available_to"]:
        return {"states": {}, "note": "after available_to (table redesign)"}
    try:
        rows = census_get(client, defn["series"], year, defn["variables"])
        header = rows[0]
        states = {}
        for row in rows[1:]:
            rd = dict(zip(header, row))
            states[rd["state"]] = {
                "name": rd["NAME"].replace(", United States", ""),
                "value": compute(defn["formula"], defn["variables"], rd),
            }
        return {"states": states}
    except httpx.HTTPStatusError as e:
        return {"states": {}, "error": "HTTP %s" % e.response.status_code}
    except Exception as e:  # noqa: BLE001 - want the year to fail soft, not crash the run
        return {"states": {}, "error": str(e)}

def pull_year_data(client, year):
    print("  pulling ACS %d ..." % year, flush=True)
    results = {}
    for key, defn in INDICATORS.items():
        results[key] = pull_one(client, defn, year)
        time.sleep(0.3)  # be polite to the Census API
    return {"pulled_at": datetime.now(timezone.utc).isoformat(), "indicators": results}

def year_has_any_data(year_data):
    """True if at least one indicator returned real values (i.e. the vintage exists)."""
    for ind in year_data["indicators"].values():
        if any(s.get("value") is not None for s in ind.get("states", {}).values()):
            return True
    return False

# ─────────────────────────── consistency gate ────────────────────────────────

def _median_ratio(cur_states, prev_states):
    ratios = []
    for fips, s in cur_states.items():
        v1 = s.get("value")
        v0 = prev_states.get(fips, {}).get("value")
        if v0 and v1 and v0 > 0:
            ratios.append(v1 / v0)
    return statistics.median(ratios) if ratios else None

def consistency_flags(all_years):
    """
    Scan every consecutive-year pair (per indicator) across the given
    {year: year_data} mapping. Returns a list of human-readable flags for any
    pair whose typical value shifted implausibly — the signature of a redesign.
    """
    flags = []
    yrs = sorted(int(y) for y in all_years)
    for key in INDICATORS:
        prev = None
        for y in yrs:
            cur = all_years[str(y)]["indicators"].get(key, {}).get("states", {})
            if not any(s.get("value") is not None for s in cur.values()):
                continue
            if prev is not None:
                prev_states = all_years[str(prev)]["indicators"].get(key, {}).get("states", {})
                mr = _median_ratio(cur, prev_states)
                if mr is not None and (mr > YOY_HI or mr < YOY_LO):
                    flags.append("%s: %d->%d typical value shifted %.2fx"
                                 % (INDICATORS[key]["label"], prev, y, mr))
            prev = y
    return flags

# ─────────────────────────── metadata ────────────────────────────────────────

def build_meta(years_present):
    return {
        "years_available": sorted(int(y) for y in years_present),
        "latest_year": max(int(y) for y in years_present) if years_present else None,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "survey": "ACS 1-year estimates",
        "indicators": {
            k: {
                **{k2: v[k2] for k2 in ["label", "category", "unit", "description", "higher_is", "table"]},
                "available_from": v.get("available_from", 2005),
                **({"available_to": v["available_to"]} if v.get("available_to") is not None else {}),
            }
            for k, v in INDICATORS.items()
        },
    }

# ─────────────────────────── github I/O ──────────────────────────────────────

def gh_headers():
    return {"Authorization": "token %s" % GITHUB_TOKEN, "Accept": "application/vnd.github+json"}

def read_github(client):
    url = "https://api.github.com/repos/%s/contents/%s" % (GITHUB_REPO, DATA_FILE_PATH)
    r = client.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    d = r.json()
    return json.loads(base64.b64decode(d["content"]).decode()), d["sha"]

def write_github(client, payload, message, sha):
    url = "https://api.github.com/repos/%s/contents/%s" % (GITHUB_REPO, DATA_FILE_PATH)
    body = {
        "message": message,
        "content": base64.b64encode(json.dumps(payload, indent=2, ensure_ascii=False).encode()).decode(),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha
    r = client.put(url, headers=gh_headers(), json=body, timeout=60)
    r.raise_for_status()
    return r.json()["commit"]["sha"]

# ─────────────────────────── year selection ──────────────────────────────────

def available_years_through(latest):
    return [y for y in range(START_YEAR, latest + 1) if y != 2020]

def candidate_latest_year():
    """
    ACS 1-year data for calendar year Y is released in September of Y+1, so the
    newest vintage that could exist right now is (current year - 1). 2020 has no
    ACS 1-year release, so step back if we land on it.
    """
    y = datetime.now(timezone.utc).year - 1
    return 2019 if y == 2020 else y

# ─────────────────────────── main ────────────────────────────────────────────

def finish_no_commit(reason):
    print("Nothing committed: %s" % reason, flush=True)
    sys.exit(0)

def fail(reason):
    sys.stderr.write("::error::%s\n" % reason)
    sys.exit(1)

def commit(client, payload, message):
    payload["meta"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    _, sha = read_github(client)  # re-read for the freshest sha right before writing
    new_sha = write_github(client, payload, message, sha)
    years = payload["meta"]["years_available"]
    print("Committed %s  (%d–%d, %d years)  commit %s"
          % (DATA_FILE_PATH, min(years), max(years), len(years), new_sha[:10]), flush=True)

def main():
    print("refresh_data.py starting", flush=True)
    print("  repo=%s  file=%s  branch=%s" % (GITHUB_REPO, DATA_FILE_PATH, GITHUB_BRANCH), flush=True)
    mode = "PULL_ALL" if PULL_ALL else ("SPECIFIC_YEAR=%s" % SPECIFIC_YEAR if SPECIFIC_YEAR else "LATEST")
    print("  mode=%s" % mode, flush=True)

    client = httpx.Client()
    existing, _ = read_github(client)
    existing_years = existing.get("years", {}) if existing else {}

    # ---- MODE 1: full rebuild ----
    if PULL_ALL:
        latest = candidate_latest_year()
        years = available_years_through(latest)
        print("Full rebuild: attempting %d..%d" % (years[0], years[-1]), flush=True)
        built = {}
        for y in years:
            yd = pull_year_data(client, y)
            if year_has_any_data(yd):
                built[str(y)] = yd
            else:
                print("  %d has no data yet (skipping)" % y, flush=True)
        if not built:
            fail("Full rebuild produced no data at all — check CENSUS_API_KEY and API availability.")
        flags = consistency_flags(built)
        if flags:
            for f in flags:
                sys.stderr.write("::warning::consistency flag: %s\n" % f)
            fail("Consistency check tripped on %d indicator pair(s) — a Census table "
                 "definition may have changed. Nothing committed; review the flags above "
                 "and update available_from/available_to before re-running." % len(flags))
        payload = {"meta": build_meta(list(built.keys())), "years": built}
        commit(client, payload, "ACS full rebuild [%s]" % datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        return

    # ---- MODE 2: specific year ----
    if SPECIFIC_YEAR:
        try:
            year = int(SPECIFIC_YEAR)
        except ValueError:
            fail("SPECIFIC_YEAR is not a valid year: %r" % SPECIFIC_YEAR)
        if year == 2020:
            finish_no_commit("2020 has no ACS 1-year estimates.")
        yd = pull_year_data(client, year)
        if not year_has_any_data(yd):
            finish_no_commit("ACS %d returned no data (not released yet, or unavailable)." % year)
        if not existing:
            payload = {"meta": build_meta([str(year)]), "years": {str(year): yd}}
        else:
            merged = dict(existing_years)
            merged[str(year)] = yd
            payload = {"meta": build_meta(list(merged.keys())), "years": merged}
        flags = consistency_flags(payload["years"])
        if flags:
            for f in flags:
                sys.stderr.write("::warning::consistency flag: %s\n" % f)
            fail("Consistency check tripped after adding %d — nothing committed. Review flags above." % year)
        commit(client, payload, "ACS %d refresh [%s]" % (year, datetime.now(timezone.utc).strftime("%Y-%m-%d")))
        return

    # ---- MODE 3: latest (default, for the annual schedule) ----
    latest = candidate_latest_year()
    if existing and str(latest) in existing_years:
        finish_no_commit("latest year %d is already in the file." % latest)
    yd = pull_year_data(client, latest)
    if not year_has_any_data(yd):
        finish_no_commit("ACS %d not released yet — will try again on the next run." % latest)
    if not existing:
        payload = {"meta": build_meta([str(latest)]), "years": {str(latest): yd}}
    else:
        merged = dict(existing_years)
        merged[str(latest)] = yd
        payload = {"meta": build_meta(list(merged.keys())), "years": merged}
    flags = consistency_flags(payload["years"])
    if flags:
        for f in flags:
            sys.stderr.write("::warning::consistency flag: %s\n" % f)
        fail("Consistency check tripped after adding %d — nothing committed. "
             "A Census table definition may have changed; review before publishing." % latest)
    commit(client, payload, "ACS %d refresh [%s]" % (latest, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

if __name__ == "__main__":
    main()
