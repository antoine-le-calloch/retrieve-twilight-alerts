import os
import pandas as pd

from pathlib import Path
from astropy.time import Time

from utils.config import get_config
from utils.format import format_time
from utils.kowalski import run_queries

cfg = get_config()

def queries_for_twilight_obs(start_time: Time) -> list:
    """Create a query to fetch twilight observations from Kowalski.
    Args:
        start_time (astropy.time.Time): Only observations made after this time will be included.
    Returns:
        list: List of queries to be run in Kowalski
    """
    n_processes = cfg.get("kowalski.n_processes") or 1
    queries = []
    start_jd = start_time.jd
    step = (Time.now().jd - start_jd) / n_processes
    for i in range(n_processes):
        jd_filter = {"$gte": start_jd}
        if i < n_processes - 1:
            jd_filter["$lt"] = start_jd + step
        query = {
            "query_type": "find",
            "query": {
                "catalog": "ZTF_ops",
                "filter": {
                    "qcomment": {"$in": ["MSIP_Twilight", "Partnership_Twilight"]},
                    "jd_start": jd_filter,
                },
                "projection": {
                    "_id": 0,
                    "jd_start": 1,
                    "jd_end": 1,
                    "fileroot": 1,
                },
            },
        }
        queries.append(query)
        start_jd = start_jd + step
    return queries


def fetch_twilight_obs(kowalski, start_time: Time) -> Path:
    """Fetch twilight observations from Kowalski and save them to a parquet file.
    Args:
        kowalski (Kowalski): Kowalski client
        start_time (astropy.time.Time): Only observations made after this time will be included.

    Returns:
        Path: path to the parquet file with the observations
    """
    file_name = f'extracted_obs_jd_{format_time(start_time)}.parquet'
    file_path = Path(cfg["path_to.obs"]) / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if os.path.exists(file_path):
        print(f"File {file_path} already created. Skipping fetching observations.")
        return file_path

    results = run_queries(
        kowalski=kowalski,
        queries=queries_for_twilight_obs(start_time),
    )
    results = sorted(results, key=lambda x: x["jd_start"])
    obs_info = [{
        "jd_start": obs["jd_start"],
        "jd_end": obs["jd_end"],
        "fileroot": obs["fileroot"]
    }for obs in results]
    pd.DataFrame(obs_info).to_parquet(file_path, index=False)

    print(f"Observations fetched and saved to {file_path}")
    return file_path
