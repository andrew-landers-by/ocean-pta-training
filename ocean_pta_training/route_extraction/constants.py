from typing import Final

CONFIG_FILE_DEFAULT_FILENAME: Final = "config.yaml"

# Config Keys
JOBS: Final = "JOBS"
JOB_NAME: Final = "name"
JOB_ORIGIN: Final = "origin"
JOB_DESTINATION: Final = "destination"
LOCAL_MATERIAL_ROOT_DIR: Final = "LOCAL_MATERIAL_ROOT_DIR"
LOCAL_OUTPUT_ROOT_DIR: Final = "LOCAL_OUTPUT_ROOT_DIR"
OUTPUT_TRAINING_FILE_SUBDIR: Final = "od_extracts"
OUTPUT_STATS_SUBDIR: Final = "od_stats"


# Uncategorized constants
IMO: Final = "IMO"
JOURNEY_BREAKER: Final = "journey_breaker"
MAPPED_PORT: Final = "mapped_stopped_closest_port"
PORT: Final = "stopped_closest_port"
RANGE_START: Final = "range_start"
RANGE_LENGTH: Final = "range_len"
TIME_POSITION: Final = "TimePosition"
