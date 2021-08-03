import os
import sys

class Environment:

    PATH = ".env"

    class Vars:
        CONFIG_PATH = "CONFIG_PATH"
        PATH_TO_PORTS_FILE = "PATH_TO_PORTS_FILE"
        PATH_TO_VESSEL_MOVEMENTS_DATA = "PATH_TO_VESSEL_MOVEMENTS_DATA"
        PATH_TO_OD_FILE = "PATH_TO_OD_FILE"
        PATH_TO_OUTPUT_DIRECTORY = "PATH_TO_OUTPUT_DIRECTORY"
        PATH_TO_LOG_FILE_DIRECTORY = "PATH_TO_LOG_FILE_DIRECTORY"

    @classmethod
    def set(cls):
        env_path: str = sys.argv[1].strip() if len(sys.argv) > 1 else cls.PATH
        with open(env_path, 'r') as env_file:
            for line in env_file.readlines():
                var, value = line.strip().split("=")
                os.environ[var] = value
