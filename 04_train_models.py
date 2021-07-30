#!venv/bin/python
import logging
import os
from ocean_pta_training import Environment, ModelTrainer

def main():
    # .env file is read from sys.argv[1], if given. See env.py for the default location of the .env file.
    Environment.set()
    logger = logging.getLogger(__name__)
    logger.info("Training models")

    try:
        trainer = ModelTrainer(
            material_root_dir=os.getenv(Environment.Vars.PATH_TO_OUTPUT_DIRECTORY)
        )
        trainer.train()

    except Exception as e:
        logger.exception(f"Error: {e}")


if __name__ == "__main__":
    main()
