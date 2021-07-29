import logging
import os.path
from env import EnvVars, set_env
from ocean_pta_training import ModelTrainer

def main():
    set_env()
    logger = logging.getLogger(__name__)
    logger.info("Training models")

    try:
        trainer = ModelTrainer(
            material_root_dir=os.getenv(EnvVars.PATH_TO_OUTPUT_DIRECTORY.value)
        )
        trainer.train()

    except Exception as e:
        logger.exception(f"Error: {e}")


if __name__ == "__main__":
    main()
