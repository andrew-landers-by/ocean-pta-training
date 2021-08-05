#!venv/bin/python
import logging
import os
from ocean_pta_training import Environment, ModelTrainer, OriginDestinationRouteExtractor

def main():
    # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    Environment.set()
    logger = logging.getLogger(__name__)

    # Extract training data for the specified origin-destination routes
    try:
        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.getenv(Environment.Vars.PATH_TO_PORTS_FILE),
            path_to_vessel_movements_data=os.getenv(Environment.Vars.PATH_TO_VESSEL_MOVEMENTS_DATA),
            path_to_od_file=os.getenv(Environment.Vars.PATH_TO_OD_FILE),
            path_to_output_dir=os.getenv(Environment.Vars.PATH_TO_OUTPUT_DIRECTORY),
            config_path=os.getenv(Environment.Vars.CONFIG_PATH)
        )
        feature_extractor.run()

        # Train models
        trainer = ModelTrainer(
            material_root_dir=os.getenv(Environment.Vars.PATH_TO_OUTPUT_DIRECTORY),
            config=feature_extractor.config
        )
        trainer.train()

    except Exception as e:
        message = f"An unexpected error occurred: {e}"
        logger.exception(message)


if __name__ == "__main__":
    main()
