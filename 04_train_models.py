#!venv/bin/python
import logging
import os
from env import EnvVars, set_env
from ocean_pta_training import ModelTrainer

def main():
    set_env()  # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    logger = logging.getLogger(__name__)
    logger.info("Training models")

    try:
        trainer = ModelTrainer(
            material_root_dir=os.getenv(EnvVars.PATH_TO_OUTPUT_DIRECTORY)
        )
        trainer.train()

    except Exception as e:
        logger.exception(f"Error: {e}")


if __name__ == "__main__":
    main()
