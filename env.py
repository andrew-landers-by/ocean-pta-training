from enum import Enum
import os

ENV_PATH: str = ".env"

class EnvVars(Enum):
    CONFIG_PATH = "CONFIG_PATH"
    PATH_TO_PORTS_FILE = "PATH_TO_PORTS_FILE"
    PATH_TO_VESSEL_MOVEMENTS_DATA = "PATH_TO_VESSEL_MOVEMENTS_DATA"
    PATH_TO_OD_FILE = "PATH_TO_OD_FILE"
    PATH_TO_OUTPUT_DIRECTORY = "PATH_TO_OUTPUT_DIRECTORY"

def set_env(env_path: str = ENV_PATH):
    """
    Set environment variables. Order of preference for getting variables:
    1) command line argument
    2) .env file in the working directory
    """
    # TODO: skipping command line arg logic for now
    with open(env_path, 'r') as env_file:
        for line in env_file.readlines():
            var, value = line.strip().split("=")
            os.environ[var] = value
