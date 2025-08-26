import json

from pathlib import Path
from astropy.time import Time

from utils.config import get_config
from utils.format import format_time, parse_time
from utils.kowalski import run_queries

cfg = get_config()

def queries_for_twilight_alerts(twilight_obs_info: list, start_time: Time, end_time: Time) -> list:
    """Create a query to fetch twilight alerts from Kowalski.
    Args:
        twilight_obs_info (list): List of dicts with keys 'jd_start', 'jd_end' and 'fileroot'
        start_time (astropy.time.Time): Only alerts made after this time will be included.
        end_time (astropy.time.Time): Only alerts made before this time will be included.
    Returns:
        list: List of queries to be run in Kowalski
    """
    queries = []
    cpt = 0
    for obs in twilight_obs_info:
        if obs['jd_start'] >= start_time.jd:
            break
        cpt += 1

    while cpt < len(twilight_obs_info) and twilight_obs_info[cpt]['jd_start'] <= end_time.jd:
        # get the start of the first twilight observation
        jd_start = twilight_obs_info[cpt]['jd_start']

        # regroup intervals when the gap between them is less than 30 seconds (same twilight session)
        while (cpt < len(twilight_obs_info) - 1 and
            twilight_obs_info[cpt+1]['jd_start'] - twilight_obs_info[cpt]['jd_end'] < 0.000347222):
            if twilight_obs_info[cpt]['jd_start'] > end_time.jd:
                break
            cpt += 1

        # get the start of the last twilight observation of this session
        jd_end = twilight_obs_info[cpt]['jd_start']
        cpt += 1

        queries.append({
            "query_type": "find",
            "query": {
                "catalog": "ZTF_alerts",
                "filter": {
                    "candidate.jd": {
                        "$gte": jd_start - 0.000075,
                        "$lte": jd_end,
                    }
                },
                "projection": {
                    "_id": 0,
                    "candid": 1,
                    "objectId": 1,
                    "candidate.jd": 1,
                    "candidate.ra": 1,
                    "candidate.dec": 1,
                    "candidate.magpsf": 1,
                    "candidate.sigmapsf": 1,
                    "candidate.pdiffimfilename": 1,
                },
            },
        })

    nb_twilight_obs_to_process = sum(
        start_time.jd <= obs['jd_start'] <= end_time.jd
        for obs in twilight_obs_info
    )
    print(f"Found {len(queries)} queries for {nb_twilight_obs_to_process} twilight observations to process")
    return queries

def filter_twilight_alerts(alerts: list, obs_list: list):
    """Filter alerts to make sure they are from twilight observations.
    Args:
        alerts (list): List of alerts from Kowalski
        obs_list (list): List of dicts with keys 'jd_start', 'jd_end' and 'fileroot'
    """
    cpt = 0
    nb_twilight_alerts = 0
    twilight_alerts = []
    seen_ids = set()
    for alert in alerts:
        while alert['candidate']['jd'] > obs_list[cpt]['jd_start'] and cpt < len(obs_list) - 1:
            cpt += 1

        if obs_list[cpt]['fileroot'] in alert['candidate']['pdiffimfilename']:
            if alert['candid'] in seen_ids:
                print(f"Duplicate alert {alert['candid']} found!")
                continue

            nb_twilight_alerts += 1
            twilight_alerts.append({
                "objectId": alert['objectId'],
                "jd": alert['candidate']['jd'],
                "ra": alert['candidate']['ra'],
                "dec": alert['candidate']['dec'],
                "magpsf": alert['candidate']['magpsf'],
                "sigmapsf": alert['candidate']['sigmapsf'],
            })
            seen_ids.add(alert['candid'])

    print(f"Found {nb_twilight_alerts} twilight alerts in {len(alerts)} alerts")
    return twilight_alerts

def fetch_twilight_alerts(
        kowalski,
        twilight_obs_info: list,
        start_time: Time = Time("2018-11-01T00:00:00.000", scale="utc"),
        end_time: Time = Time.now()
) -> Path:
    """Fetch twilight alerts from Kowalski

    Args:
        kowalski (Kowalski): Kowalski client
        twilight_obs_info (list): List of dicts with keys 'jd_start', 'jd_end' and 'fileroot'
        start_time (astropy.time.Time, optional): Only alerts made after this time will be included.
            Defaults to "2018-11-01T00:00:00.000".
        end_time (astropy.time.Time, optional): Only alerts made before this time will be included.
            Defaults to now.
    Returns:
        Path: path to the JSON file with the alerts
    """
    file_name = f'twilight_alerts_{format_time(start_time)}_{format_time(end_time)}.json'
    file_path = Path(cfg["path_to.alerts"]) / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        print(f"File {file_path.name} already created. Skipping fetching alerts.")
        return file_path
    else:
        matching_files = list(file_path.parent.glob(f"twilight_alerts_{format_time(start_time)}*.json"))
        if matching_files:
            # If files exists with the same start time, take the one with the latest end time
            # And skip this if end time is more recent; otherwise start using this end time
            matching_files.sort(reverse=True)
            match_file = matching_files[0]
            file_end_time = parse_time(matching_files[0].stem.split(f"{format_time(start_time)}_")[-1])
            if file_end_time >= end_time:
                print(f"File {match_file.name} already created with a more recent end time. Skipping fetching alerts.")
                return match_file
            else:
                print(f"File {match_file.name} already created with the same start time but an older end time."
                      f" Create a new file with its end time as starting time.")
                file_path = file_path.parent / file_name.replace(format_time(start_time), format_time(file_end_time))
                start_time = file_end_time

    results = run_queries(
        kowalski=kowalski,
        queries=queries_for_twilight_alerts(twilight_obs_info, start_time, end_time)
    )

    results = filter_twilight_alerts(
        sorted(results, key=lambda x: x["candidate"]["jd"]),
        twilight_obs_info
    )

    print(f"Writing {len(results)} results to {file_path.name}...")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Alerts successfully fetched and saved\n")
    return file_path
