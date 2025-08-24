import os
import pandas as pd

from pathlib import Path
from astropy.time import Time

from fetch_twilight_alerts import fetch_twilight_alerts
from fetch_twilight_obs import fetch_twilight_obs
from utils.config import get_config
from utils.kowalski import get_kowalski

cfg = get_config()

def load_twilight_obs_info(parquet_file: Path) -> list:
    """Load twilight observations info from a parquet file.

    Args:
        parquet_file (Path): Path to the parquet file

    Returns:
        list: List of dicts with keys 'jd_start', 'jd_end' and 'fileroot'
    """
    if not os.path.exists(parquet_file) or not os.path.isfile(parquet_file):
        raise FileNotFoundError(f"File {parquet_file} does not exist or is not a file.")

    df = pd.read_parquet(parquet_file)
    if df.empty:
        raise ValueError(f"File {parquet_file} is empty.")

    return df[['jd_start', 'jd_end', 'fileroot']].to_dict(orient='records')

if __name__ == "__main__":
    fetch_twilight_obs_after = Time("2018-11-01T00:00:00.000", scale="utc")
    fetch_twilight_alerts_after = Time("2024-12-18T09:02:53.000", scale="utc")
    kowalski = get_kowalski(verbose=True)

    # Fetch twilight observations info and save them to a parquet file
    print("Starting twilight observations fetching...")
    parquet_file_path = fetch_twilight_obs(kowalski, fetch_twilight_obs_after)

    twilight_obs_info = load_twilight_obs_info(parquet_file_path)
    print(f"\nLoaded {len(twilight_obs_info)} twilight observations")

    # Fetch twilight alerts based on the observation info in the parquet file
    days_per_step = cfg.get("parameters.days_per_step")
    start_time = fetch_twilight_alerts_after
    print("\nStarting twilight alerts fetching...\n")

    if not days_per_step:
        print(f"Processing time range: {start_time.iso} to now")
        alerts_file = fetch_twilight_alerts(
            kowalski,
            twilight_obs_info,
            start_time,
        )
    else:
        while start_time <= Time.now():
            end_time = Time(start_time.jd + days_per_step, format="jd", scale="utc")
            print(f"Processing time range: {start_time.iso} to {end_time.iso}")
            alerts_file = fetch_twilight_alerts(
                kowalski,
                twilight_obs_info,
                start_time,
                end_time if end_time <= Time.now() else Time.now(),
            )
            start_time = end_time.iso

    print("\nFinished.")