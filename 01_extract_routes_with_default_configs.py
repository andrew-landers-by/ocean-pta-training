import logging
import os
from env import EnvVars, set_env
from ocean_pta_training import OriginDestinationRouteExtractor

def main():
    set_env()
    logger = logging.getLogger(__name__)
    logger.info("BEGINNING AN INCREMENTAL DATA EXTRACTION PROCESS FOR OCEAN PTA")

    try:
        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.getenv(EnvVars.PATH_TO_PORTS_FILE.value),
            path_to_vessel_movements_data=os.getenv(EnvVars.PATH_TO_VESSEL_MOVEMENTS_DATA.value),
            path_to_od_file=os.getenv(EnvVars.PATH_TO_OD_FILE.value),
            path_to_output_dir=os.getenv(EnvVars.PATH_TO_OUTPUT_DIRECTORY.value),
            config_path=os.getenv(EnvVars.CONFIG_PATH.value)
        )
        feature_extractor.run()
    except Exception as e:
        logger.exception(f"Error: {e}")


if __name__ == "__main__":
    main()
