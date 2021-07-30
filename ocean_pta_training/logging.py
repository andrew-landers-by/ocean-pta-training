import logging
import os
import sys
from datetime import date
from typing import Optional
from . import Environment, configs, ConfigKeys

logger = logging.getLogger(__name__)

def get_logging_level() -> int:
    """
    Read in the logging level from configs
    """
    valid_choices = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
    level: str = (
        configs.get(ConfigKeys.LOGGING)
        .get(ConfigKeys.LOGGING_LEVEL)
    )
    if level:
        upper_level: str = level.upper()

        if upper_level == "CRITICAL":
            return logging.CRITICAL
        elif upper_level == "ERROR":
            return logging.ERROR
        elif upper_level == "WARNING":
            return logging.WARNING
        elif upper_level == "INFO":
            return logging.INFO
        elif upper_level == "DEBUG":
            return logging.DEBUG
        else:
            message = f"Invalid logging level {level} was read in from config.yaml; expected one of {valid_choices}"
    else:
        message = f"Failed to read in a logging level from config.yaml. Check whether it is blank. Expected one of {valid_choices}"  # noqa

    if message:
        logger.error(message)
        raise ValueError(message)

def generate_logging_file_name() -> str:
    """The name of the logging file """
    log_file_prefix = "ocean_pta_training"
    today = date.today()
    month = today.month if today.month >= 10 else f"0{today.month}"
    day = today.day if today.day >= 10 else f"0{today.day}"
    return f"{log_file_prefix}_{today.year}_{month}_{day}.log"


def get_logging_file_path() -> Optional[str]:
    """Retrieve the local logging file"""
    logging_dirname = os.getenv(Environment.Vars.PATH_TO_LOG_FILE_DIRECTORY)
    if logging_dirname:
        if os.path.isdir(logging_dirname):
            return os.path.join(logging_dirname, generate_logging_file_name())
        else:
            message = f"WARNING: No file handler was created because the specified directory {logging_dirname} does not exist on the file system!"
            print(message)
            return  # Don't raise exception for this; just print the warning message. We don't log the message because logging isn't set up yet
    else:
        return  # return nothing: if the variable isn't defined, we don't log to a file.

def set_logging_config():
    """Set logging level. We get this from configs."""
    # PyCharm's linter doesn't recognize the kwarg 'handlers'...
    handlers = [logging.StreamHandler(sys.stderr)]
    logging_fpath = get_logging_file_path()
    if logging_fpath:
        handlers.append(logging.FileHandler(logging_fpath))

    # PyCharm's linter doesn't recognize the kwarg 'handlers'.
    # It is used to log to both stderr and to file.
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=get_logging_level(),
        datefmt="%H:%M:%S",
        handlers=handlers
    )


set_logging_config()
