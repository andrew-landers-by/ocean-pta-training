import logging
import os
import pandas as pd
import yaml
from ocean_pta_training import Environment
from typing import Dict

logger = logging.getLogger(f"{__name__}")


def main():
    Environment.set()                                   # Read in environment variables from .env file
    routes_df = read_in_od_jobs_from_csv()              # Read in new jobs
    config_dict = generate_yaml_config_dict(routes_df)  # Create a config dict from these jobs
    write_config_yaml(config_dict)                      # Write config dict to YAML file


def write_config_yaml(config_dict: Dict) -> None:
    """Write config dict data to a YAML file"""
    config_yaml_file_path = os.environ.get(Environment.Vars.CONFIG_PATH)
    logger.info(f"Writing config dict to YAML file: {config_yaml_file_path}")
    with open(config_yaml_file_path, 'w') as yaml_file:
        yaml.dump(config_dict, yaml_file)
    logger.info("...done")


def read_in_od_jobs_from_csv() -> pd.DataFrame:
    """Read in list of jobs from CSV, return as a DataFrame"""
    csv_file_path = os.environ.get(Environment.Vars.PATH_TO_NEW_OD_REQUIREMENT_CSV_FILE)
    logger.info(f"Reading OD jobs from CSV file: {csv_file_path}")
    with open(csv_file_path, 'r') as csv_file:
        df = pd.read_csv(csv_file)

    logger.info(f"...successfully read in {len(df.index)} ODs from CSV file")
    return df


def generate_yaml_config_dict(routes_df: pd.DataFrame):
    """Extract a config dict from the contents of DataFrame routes_df"""
    logger.info("Creating a config dict using these requirements")
    config_dict = {"JOBS": {}}
    jobs_list = routes_df.apply(
        lambda row: extract_an_od_job_dict(row),
        axis=1
    )
    for job in jobs_list:
        route_key = job.get("key")
        route_data = job.get("data")
        config_dict["JOBS"][route_key] = route_data

    n_jobs = len(config_dict.get("JOBS"))
    logger.info(f"...generated a config dict specifying {n_jobs} jobs")
    return config_dict


def extract_an_od_job_dict(row) -> Dict:
    """Take in a row of previously read-in data and extract a dict representing a single job"""
    origin_port = row.get("origin_port")
    destination_port = row.get("destination_port")

    return {
        "key": f"{origin_port}-{destination_port}",
        "data": {
            "origin": row.get("origin_port"),
            "destination": row.get("destination_port")
        }
    }


if __name__ == "__main__":
    main()
