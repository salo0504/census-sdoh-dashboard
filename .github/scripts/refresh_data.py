"""
.github/scripts/refresh_data.py
================================
Run by GitHub Actions to pull ACS data and commit to the repo.
Called automatically each September or manually via workflow_dispatch.
"""

import os, json, asyncio, base64, logging
from datetime import datetime, timezone
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CENSUS_API_KEY = os.environ["CENSUS_API_KEY"]
GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GITHUB_REPO    = os.environ["GITHUB_REPO"]
GITHUB_BRANCH  = os.environ.get("GITHUB_BRANCH", "main")
DATA_FILE_PATH = "data/data_all_years.json"
PULL_ALL       = os.environ.get("PULL_ALL", "false").lower() == "true"
SPECIFIC_YEAR  = os.environ.get("SPECIFIC_YEAR", "").strip()

# ACS 1-year available 2005-present; no 2020
AVAILABLE_YEARS = [y for y in range(2005, 2025) if y != 2020]

INDICATORS = {
    "median_household_income":   {"table":"B19013","variables":["B19013_001E"],"formula":"single","unit":"dollars","series":"acs1","label":"Median Household Income","category":"Economic Stability","description":"Median household income (inflation-adjusted)","higher_is":"better","available_from":2005},
    "poverty_rate":              {"table":"S1701","variables":["S1701_C03_001E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"Poverty Rate","category":"Economic Stability","description":"Percent below poverty level","higher_is":"worse","available_from":2005},
    "employment_rate":           {"table":"S2301","variables":["S2301_C03_001E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"Employment Rate","category":"Economic Stability","description":"Employment-population ratio, 16+","higher_is":"better","available_from":2005},
    "snap_receipt_rate":         {"table":"B22003","variables":["B22003_002E","B22003_001E"],"formula":"percent_of_total","unit":"percent","series":"acs1","label":"SNAP Receipt Rate","category":"Economic Stability","description":"% households receiving SNAP","higher_is":"worse","available_from":2005},
    "hs_diploma_rate":           {"table":"S1501","variables":["S1501_C02_014E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"High School Diploma or Higher","category":"Education","description":"% population 25+ with HS diploma","higher_is":"better","available_from":2005},
    "bachelors_rate":            {"table":"S1501","variables":["S1501_C02_015E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"Bachelor's Degree or Higher","category":"Education","description":"% population 25+ with bachelor's","higher_is":"better","available_from":2005},
    "uninsured_rate":            {"table":"S2701","variables":["S2701_C05_001E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"Uninsured Rate (Under 65)","category":"Healthcare Access","description":"% under 65 without health insurance","higher_is":"worse","available_from":2010},
    "disability_rate":           {"table":"S1810","variables":["S1810_C03_001E"],"formula":"single","unit":"percent","series":"acs1/subject","label":"Disability Rate","category":"Healthcare Access","description":"% population with a disability","higher_is":"neutral","available_from":2005},
    "median_gross_rent":         {"table":"B25064","variables":["B25064_001E"],"formula":"single","unit":"dollars","series":"acs1","label":"Median Gross Rent","category":"Housing","description":"Median gross rent per month","higher_is":"neutral","available_from":2005},
    "homeownership_rate":        {"table":"B25003","variables":["B25003_002E","B25003_001E"],"formula":"percent_of_total","unit":"percent","series":"acs1","label":"Homeownership Rate","category":"Housing","description":"% occupied units owner-occupied","higher_is":"better","available_from":2005},
    "housing_cost_burden":       {"table":"B25070","variables":["B25070_007E","B25070_008E","B25070_009E","B25070_010E","B25070_001E"],"formula":"sum_over_total","unit":"percent","series":"acs1","label":"Housing Cost Burden (Renters)","category":"Housing","description":"% renters paying 30%+ on housing","higher_is":"worse","available_from":2005},
    "foreign_born_pct":          {"table":"B05002","variables":["B05002_013E","B05002_001E"],"formula":"percent_of_total","unit":"percent","series":"acs1","label":"Foreign-Born Population","category":"Community Context","description":"% population foreign-born","higher_is":"neutral","available_from":2005},
    "non_english_household_pct": {"table":"B16010","variables":["B16010_002E","B16010_001E"],"formula":"percent_of_total","unit":"percent","series":"acs1","label":"Non-English Speaking Households","category":"Community Context","description":"% households non-English speaking","higher_is":"neutral","available_from":2010},
    "single_parent_pct":         {"table":"B11012","variables":["B11012_010E","B11012_015E","B11012_001E"],"formula":"sum_over_total","unit":"percent","series":"acs1","label":"Single-Parent Households","category":"Community Context","description":"% family households single-parent","higher_is":"neutral","available_from":2005},
    "broadband_access":          {"table":"B28002","variables":["B28002_004E","B28002_001E"],"formula":"percent_of_total","unit":"percent","series":"acs1","label":"Broadband Internet Access","category":"Broadband Access","description":"% households with broadband","higher_is":"better","available_from":2013},
}

SUPPRESSION = {"-666666666","-999999999","-888888888","N","NA","(X)","null","None"}

def safe_val(v):
    s = str(v)
    if s in SUPPRESSION or (s.startswith("-") and len(s) > 1):
        return None
    try: return float(v)
    except: return None

def compute(formula, variables, row):
    if formula == "single":
        return safe_val(row.get(variables[0]))
    elif formula == "percent_of_total":
        n, d = safe_val(row.get(variables[0])), safe_val(row.get(variables[1]))
        return round(n/d*100, 2) if n is not None and d else None
    elif formula == "sum_over_total":
        parts = [safe_val(row.get(v)) for v in variables[:-1]]
        d = safe_val(row.get(variables[-1]))
        return round(sum(parts)/d*100, 2) if all(p is not None for p in parts) and d else None
    return None

async def census_get(session, series, year, variables):
    url = f"https://api.census.gov/data/{year}/acs/{series}"
    params = {"get": "NAME,"+",".join(variables), "for": "state:*", "key": CENSUS_API_KEY}
    r = await session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

async def pull_one(session, key, defn, year):
    if year < defn.get("available_from", 2005):
        return {"states": {}, "note": f"Not available before {defn['available_from']}"}
    try:
        rows = await census_get(session, defn["series"], year, defn["variables"])
        header = rows[0]
        states = {}
        for row in rows[1:]:
            rd = dict(zip(header, row))
            states[rd["state"]] = {"name": rd["NAME"].replace(", United States",""), "value": compute(defn["formula"], defn["variables"], rd)}
        return {"states": states}
    except httpx.HTTPStatusError as e:
        log.error(f"  {key} {year}: {e}")
        return {"states": {}, "error": str(e)}

async def pull_year_data(session, year):
    log.info(f"Pulling ACS {year}...")
    results = {}
    for key, defn in INDICATORS.items():
        results[key] = await pull_one(session, key, defn, year)
        await asyncio.sleep(0.35)
    log.info(f"  ACS {year} done")
    return {"pulled_at": datetime.now(timezone.utc).isoformat(), "indicators": results}

def build_meta(years_present):
    return {
        "years_available": sorted(years_present),
        "latest_year": max(years_present) if years_present else None,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "survey": "ACS 1-year estimates",
        "indicators": {k: {f: v[f] for f in ["label","category","unit","description","higher_is","table","available_from"]} for k,v in INDICATORS.items()}
    }

async def read_github(session):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{DATA_FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    r = await session.get(url, headers=headers, params={"ref": GITHUB_BRANCH})
    if r.status_code == 404: return None, None
    r.raise_for_status()
    d = r.json()
    return json.loads(base64.b64decode(d["content"]).decode()), d["sha"]

async def write_github(session, payload, message, sha):
    content = base64.b64encode(json.dumps(payload, indent=2, ensure_ascii=False).encode()).decode()
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{DATA_FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    body = {"message": message, "content": content, "branch": GITHUB_BRANCH}
    if sha: body["sha"] = sha
    r = await session.put(url, headers=headers, json=body)
    r.raise_for_status()
    sha_out = r.json()["commit"]["sha"]
    log.info(f"Committed: {sha_out[:10]}")
    return sha_out

async def main():
    async with httpx.AsyncClient(timeout=60) as session:
        existing, sha = await read_github(session)
        all_data = existing or {"meta": {}, "years": {}}
        years_present = set(all_data.get("meta", {}).get("years_available", []))

        if SPECIFIC_YEAR:
            years_to_pull = [int(SPECIFIC_YEAR)]
        elif PULL_ALL:
            years_to_pull = AVAILABLE_YEARS
        else:
            # Pull only the latest year
            latest = max(AVAILABLE_YEARS)
            if latest in years_present:
                log.info(f"ACS {latest} already present — nothing to do. Exiting.")
                return
            years_to_pull = [latest]

        done, errors = [], []
        for year in years_to_pull:
            try:
                all_data["years"][str(year)] = await pull_year_data(session, year)
                years_present.add(year)
                done.append(year)
            except Exception as e:
                log.error(f"Year {year} failed: {e}")
                errors.append(f"{year}: {e}")

        if not done:
            log.info("Nothing new to commit.")
            return

        all_data["meta"] = build_meta(list(years_present))
        msg = f"Auto ACS refresh {done} [{datetime.now(timezone.utc).strftime('%Y-%m-%d')}]"
        if errors: msg += f" ({len(errors)} errors)"
        await write_github(session, all_data, msg, sha)
        log.info(f"Done. Years in file: {sorted(years_present)}")
        if errors:
            log.warning(f"Errors: {errors}")
            raise SystemExit(1)  # Fail the action so you get notified

if __name__ == "__main__":
    asyncio.run(main())
