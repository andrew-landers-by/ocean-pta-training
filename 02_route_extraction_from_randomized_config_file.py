import logging
import os.path
import pandas as pd
import yaml
from ocean_pta_training import OriginDestinationRouteExtractor
from typing import Dict

file_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(file_dir, "data")
dev_config_path = os.path.join(file_dir, "_dev_config.yaml")
routes_data_path = os.path.join(data_dir, "od_routes_v2.csv")

def main():
    logger = logging.getLogger(__name__)
    logger.info("BEGINNING AN INCREMENTAL DATA EXTRACTION PROCESS FOR OCEAN PTA")

    try:
        config = OriginDestinationRouteExtractor.load_default_configs()
        config['JOBS'] = create_random_od_jobs(n=10, seed=64)
        write_dev_config_file(config, dev_config_path)

        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.path.join(data_dir, "ports_trimmed_modified.csv"),
            path_to_vessel_movements_data=os.path.join(data_dir, "vessel_movements_with_hexes.feather"),
            path_to_od_file=os.path.join(data_dir, "od_routes_v2.csv"),
            config_path=dev_config_path  # use default configs for dev runs
        )
        feature_extractor.run()

    except Exception as e:
        logger.exception(f"Error: {e}")


def write_dev_config_file(configs: Dict, file_path: str):
    """
    Write config dict to file
    """
    with open(file_path, "w") as config_file:
        yaml.dump(configs, config_file)

def create_random_od_jobs(n: int = 1, seed: int = 1) -> Dict:
    """
    Sample from the list of known ODs to create random sample of jobs for training file extraction
    """
    ods = pd.read_csv(routes_data_path)

    return {
        od['route']: {
            'origin': od['origin'],
            'destination': od['destination']
        }
        for od in ods.sample(n, random_state=seed).to_dict(orient="records")
    }


if __name__ == "__main__":
    main()
