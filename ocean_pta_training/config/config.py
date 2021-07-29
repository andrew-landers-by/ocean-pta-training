import logging
import os
import yaml

CONFIG_FILE_NAME = "config.yaml"

logger = logging.getLogger(__file__)

class Keys:

    # Logging related configs
    LOGGING = "LOGGING"
    LOGGING_LEVEL = "level"
    DO_LOG_TO_FILE = "log_to_file"
    LOCAL_LOG_FILE_DIRECTORY = "local_log_file_directory"

def load_config() -> dict:
    """
    Load configurations from a YAML file
    """
    yaml_file_path = os.path.join(
        os.path.dirname(__file__), CONFIG_FILE_NAME
    )
    try:
        with open(yaml_file_path, 'r') as yaml_file:
            config: dict = yaml.load(yaml_file, Loader=yaml.FullLoader)
            return config
    except yaml.YAMLError as ye:
        message = f"Unsuccessful in loading data from YAML: {ye}"
        logger.exception(message)
        raise yaml.YAMLError(message)
