# Retrieve Twilight-Kowalski Alerts

## Description
This Python project is designed to retrieve all observations from the Twilight Survey
and match them with the corresponding alerts from the Kowalski system.
The workflow consists of two main steps:
- Fetch Twilight Observations: Using `fetch_twilight_obs`, the script collects all Twilight Survey observations
after a specified date and saves the results to a Parquet file.
Each observation includes its start and end Julian dates (jd_start, jd_end) and associated `fileroot`. 
- Fetch Corresponding Kowalski Alerts: Using `fetch_twilight_alerts`,
the script iterates over the observation data and retrieves all matching alerts from Kowalski based on jd.
The results are filtered to ensure that each alert includes the associated observation's `fileroot`.
Alerts can be split in configurable time steps (days_per_step) or for the entire period at once.

## Prerequisites
Make sure you have Python installed and then install the required dependencies:
```bash
    pip install -r requirements.txt
```

## Configuration
The config.defaults.yaml file contains all the necessary configuration parameters for the scripts.
Create a config.yaml file based on config.defaults.yaml and modify it as needed.
Ensure it is correctly set up before running the project.

## Usage
To run the main script:
```bash
    python main.py
```
This will execute the process of fetching observations and retrieving alerts.
The results will be saved in the specified output directory.
If observations or alerts have already been fetched, the script will skip that step.
Delete the existing observations file to re-fetch them with the latest data.
The alerts will always be updated to ensure the latest information is retrieved.

## Todo
- [ ] Allow the observation file to update automatically when there is new data available.