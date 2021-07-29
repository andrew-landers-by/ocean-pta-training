import logging
import os.path
import pandas as pd
import yaml
from env import EnvVars, set_env
from ocean_pta_training import OriginDestinationRouteExtractor
from typing import Dict

NUMBER_OF_ROUTES: int = 5
RANDOM_SEED: int = 64

def main():
    set_env()
    logger = logging.getLogger(__name__)
    logger.info("BEGINNING AN INCREMENTAL DATA EXTRACTION PROCESS FOR OCEAN PTA")

    try:
        config = OriginDestinationRouteExtractor.load_default_configs()
        config['JOBS'] = create_random_od_jobs(n=NUMBER_OF_ROUTES, seed=RANDOM_SEED)
        write_config_file(config, os.getenv(EnvVars.CONFIG_PATH.value))

        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.getenv(EnvVars.PATH_TO_PORTS_FILE.value),
            path_to_vessel_movements_data=os.getenv(EnvVars.PATH_TO_VESSEL_MOVEMENTS_DATA.value),
            path_to_od_file=os.getenv(EnvVars.PATH_TO_OD_FILE.value),
            config_path=os.getenv(EnvVars.CONFIG_PATH.value)
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
    ods = pd.read_csv(os.getenv(EnvVars.PATH_TO_OD_FILE.value))

    return {
        od['route']: {
            'origin': od['origin'],
            'destination': od['destination']
        }
        for od in ods.sample(n, random_state=seed).to_dict(orient="records")
    }


if __name__ == "__main__":
    main()
