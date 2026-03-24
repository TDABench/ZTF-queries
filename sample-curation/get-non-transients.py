from astropy import time as astrotime
import requests
import time
import os

FRITZ_API_KEY = os.environ.get('FRITZ_API_KEY')
if FRITZ_API_KEY is None:
    host = None
    headers = None
    raise ValueError("FRITZ_API_KEY is not set")
else:
    host = "https://fritz.science"
    headers = {'Authorization': f'token {FRITZ_API_KEY}'}


def query_BTS_save_times(ZTFID):
    endpoint = host + f"/api/sources/{ZTFID}"
    r = requests.get(host + endpoint, headers=headers, params={})
    data = r.json()['data']

    for group in data['groups']:
        if group['name'] == "Redshift Completeness Factor":
            return astrotime.Time(group['saved_at']).jd

    return None


def query_not_saved_sources(exclude_BTS_junk: bool = False):
    # BTS started 2018-04-01; ended 2025-01-01
    # BTS filter last changed 2020-10-29
    # Cannot query for before 2021 (i.e. before Fritz was in use)
    start_date = "2021-01-01"
    end_date = "2025-01-01"
    BTS_group_id = "41"
    if exclude_BTS_junk:
        BTS_group_id += ",255"

    endpoint = host + "/api/candidates"

    num_per_page = 250
    objids = {}
    page_num = 1

    while True:
        print(f"Page {page_num} of not saved sources. {len(objids)} sources done so far")
        params = {
            "savedStatus": "notSavedToAnySelected",
            "startDate": start_date,
            "endDate": end_date,
            "groupIDs": BTS_group_id,
            "numPerPage": num_per_page,
            "pageNumber": page_num,
        }
        r = requests.get(endpoint, headers=headers, params=params)

        if "out of range" in r.text:
            # No page at this index (e.g. total count is an exact multiple of
            # num_per_page and the next page does not exist).
            break

        candidates = r.json()["data"]["candidates"]
        n = len(candidates)

        added_any = False
        for c in candidates:
            oid = c["id"]
            if oid not in objids:
                objids[oid] = (c["ra"], c["dec"])
                added_any = True

        # Short page => last chunk; do not request the next page (avoids OOR
        # and duplicate slices from shrinking numPerPage).
        if n < num_per_page:
            break
        if not added_any:
            break

        page_num += 1
        time.sleep(2)  # Prevent rate limiting

    return objids


if __name__ == "__main__":
    not_saved_sources = query_not_saved_sources(exclude_BTS_junk=False)
    with open("../not_saved_sources.txt", "w") as f:
        f.write("ZTFID\tra\tdec\n")
        for obj_id, (ra, dec) in not_saved_sources.items():
            f.write(f"{obj_id}\t{ra}\t{dec}\n")

    not_saved_sources = query_not_saved_sources(exclude_BTS_junk=True)
    with open("../not_saved_sources_no_junk.txt", "w") as f:
        f.write("ZTFID\tra\tdec\n")
        for obj_id, (ra, dec) in not_saved_sources.items():
            f.write(f"{obj_id}\t{ra}\t{dec}\n")
