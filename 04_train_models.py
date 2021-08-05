#!venv/bin/python
import logging
import os
import yaml
from ocean_pta_training import Environment, ModelTrainer
from typing import Dict

def main():
    # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    Environment.set()
    logger = logging.getLogger(__name__)
    logger.info("Training models")

    try:
        trainer = ModelTrainer(
            material_root_dir=os.getenv(Environment.Vars.PATH_TO_OUTPUT_DIRECTORY),
            config=load_config(os.getenv(Environment.Vars.CONFIG_PATH))
        )
        trainer.train()

    except Exception as e:
        logger.exception(f"Error: {e}")


def load_config(config_path: str) -> Dict:
    """Load training jobs from local config file"""
    with open(config_path, 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


if __name__ == "__main__":
    main()
