#!venv/bin/python
import logging
import os
import pandas as pd
import yaml
from ocean_pta_training import Environment, OriginDestinationRouteExtractor
from typing import Dict

NUMBER_OF_ROUTES: int = 5
RANDOM_SEED: int = 64

def main():
    # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    Environment.set()
    logger = logging.getLogger(__name__)

    try:
        config = OriginDestinationRouteExtractor.load_default_configs()
        config['JOBS'] = create_random_od_jobs(n=NUMBER_OF_ROUTES, seed=RANDOM_SEED)
        write_config_file(config, os.getenv(Environment.Vars.CONFIG_PATH))

        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.getenv(Environment.Vars.PATH_TO_PORTS_FILE),
            path_to_vessel_movements_data=os.getenv(Environment.Vars.PATH_TO_VESSEL_MOVEMENTS_DATA),
            path_to_od_file=os.getenv(Environment.Vars.PATH_TO_OD_FILE),
            path_to_output_dir=os.getenv(Environment.Vars.PATH_TO_OUTPUT_DIRECTORY),
            config_path=os.getenv(Environment.Vars.CONFIG_PATH)
        )
        feature_extractor.run()

    except Exception as e:
        logger.exception(f"Error: {e}")


def write_config_file(configs: Dict, file_path: str):
    """
    Write config dict to file
    """
    with open(file_path, "w") as config_file:
        yaml.dump(configs, config_file)

def create_random_od_jobs(n: int = 1, seed: int = 1) -> Dict:
    """
    Sample from the list of known ODs to create random sample of jobs for training file extraction
    """
    ods = pd.read_csv(os.getenv(Environment.Vars.PATH_TO_OD_FILE))

    return {
        od['route']: {
            'origin': od['origin'],
            'destination': od['destination']
        }
        for od in ods.sample(n, random_state=seed).to_dict(orient="records")
    }


if __name__ == "__main__":
    main()
