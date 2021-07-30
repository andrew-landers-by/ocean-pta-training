import os
import sys

ENV_PATH: str = ".env"  # define the default location of the .env file

class EnvVars:
    CONFIG_PATH = "CONFIG_PATH"
    PATH_TO_PORTS_FILE = "PATH_TO_PORTS_FILE"
    PATH_TO_VESSEL_MOVEMENTS_DATA = "PATH_TO_VESSEL_MOVEMENTS_DATA"
    PATH_TO_OD_FILE = "PATH_TO_OD_FILE"
    PATH_TO_OUTPUT_DIRECTORY = "PATH_TO_OUTPUT_DIRECTORY"

def set_env():
    """
    Set environment variables. Order of preference for getting variables:
    1) value of sys.argv[1], if given
    2) .env file in the default path
    """
    env_path: str = ENV_PATH
    if len(sys.argv) > 1:
        env_path = sys.argv[1].strip()

    with open(env_path, 'r') as env_file:
        for line in env_file.readlines():
            var, value = line.strip().split("=")
            os.environ[var] = value
