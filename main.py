#!venv/bin/python
import logging
import os
from env import EnvVars, set_env
from ocean_pta_training import ModelTrainer, OriginDestinationRouteExtractor

def main():
    set_env()  # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    logger = logging.getLogger(__name__)

    # Extract training data for the specified origin-destination routes
    try:
        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.getenv(EnvVars.PATH_TO_PORTS_FILE),
            path_to_vessel_movements_data=os.getenv(EnvVars.PATH_TO_VESSEL_MOVEMENTS_DATA),
            path_to_od_file=os.getenv(EnvVars.PATH_TO_OD_FILE),
            path_to_output_dir=os.getenv(EnvVars.PATH_TO_OUTPUT_DIRECTORY),
            config_path=os.getenv(EnvVars.CONFIG_PATH)
        )
        feature_extractor.run()

    except Exception as e:
        message = f"An unexpected error occurred while extracting the training data: {e}"
        logger.exception(message)

    # Train models
    try:
        trainer = ModelTrainer(
            material_root_dir=os.getenv(EnvVars.PATH_TO_OUTPUT_DIRECTORY)
        )
        trainer.train()

    except Exception as e:
        message = f"An unexpected error occurred while training the models: {e}"
        logger.exception(message)


def train_models():
    pass


if __name__ == "__main__":
    main()
