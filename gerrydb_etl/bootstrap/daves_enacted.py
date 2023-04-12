"""Loads enacted plans from Dave's Redistricting."""
import click
import httpx

PLANS_URL = "https://dra-us-west-datafiles-dev.s3.amazonaws.com/_state_plans.json"
EDIT_CACHE_BASE_URL = "https://dra-uswest-editcache.s3-us-west-2.amazonaws.com"
CONNECT_BASE_URL = "https://davesredistricting.org/api/sessions/connect"

# A few of the plans in the main listing (as of 2023-03-24) don't use
# the Dave's naming convention for enacted plans. We assign these plans
# custom paths.
SPECIAL_PLANS = {
    "NC 118th Congressional (Court Approved - HB 1029)": "nc_congress_hb_2019",
}


@click.command()
def main():
    """Scrapes plans from Dave's Redistricting."""
    import json

    plans_index = httpx.get(PLANS_URL).json()
    for state, state_data in plans_index.items():
        for plan in state_data["plans"]:
            print(plan)
            plan_data = httpx.post(f"{CONNECT_BASE_URL}/{plan['id']}").json()
            print(json.dumps(plan_data, indent=4))
            print(f"{EDIT_CACHE_BASE_URL}/{plan_data['editcache']}")
            try:
                response = httpx.get(f"{EDIT_CACHE_BASE_URL}/{plan_data['editcache']}")
                response.raise_for_status()
                plan_edit_cache = response.json()
                print(json.dumps(plan_edit_cache, indent=4))
            except httpx.HTTPError as ex:
                print(ex)
        break


if __name__ == "__main__":
    main()
