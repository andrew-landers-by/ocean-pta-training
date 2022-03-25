import os
import logging
import pandas as pd
from ocean_pta_training import Environment
from typing import List

OD_EXTRACTS_FILE_DIR = "/Users/Andrewlanders/projects/ocean_pta/ocean-pta-training/output/od_extracts"
DATA_DIR = "/Users/Andrewlanders/projects/ocean_pta/ocean-pta-training/output"
COMBINED_DATA_FILE_NAME = "all_od_extracts.feather"

logger = logging.getLogger(f"{__name__}")

od_extract_selected_columns = [
    'IMO', 'MMSI', 'OD', 'unique_route_ID', 'TimePosition', 'Latitude', 'Longitude',
    'is_moving', 'Speed', 'num_intermediate_ports', 'week', 'month',
    'elapsed_time', 'journey_percent', 'remaining_lead_time',
]
od_extract_columns_map = {
    'TimePosition': 'time_position',
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'Speed': 'speed',
    'unique_route_ID': 'unique_route_id'
}


def main():
    """Concatenate individual O-D extracts"""
    combined_data = pd.DataFrame()

    # Prepare the imported dataset, and concatenate it to combined_data
    for file_name in list_od_extract_file_names():
        file_path = os.path.join(OD_EXTRACTS_FILE_DIR, file_name)
        logger.info(f"Opening OD data extract from file {file_path}")
        this_od_extract = pd.read_feather(file_path)
        logger.info("...performing data preparation...")
        this_od_extract = prepare_od_extract_data(this_od_extract)
        logger.info("...concatenating this data to the combined dataset.")
        combined_data = pd.concat(
            [combined_data, this_od_extract],
            ignore_index=True
        )

    # Print a sample of rows to the terminal
    logger.info(f"First rows:\n{combined_data.head()}\n")
    logger.info(f"Last rows:\n{combined_data.tail()}\n")

    combined_data_file_path = os.environ.get(Environment.Vars.PATH_TO_COMBINED_OD_DATA)
    logger.info(f"Writing the combined dataset to file at path: {combined_data_file_path}")
    combined_data.to_feather(combined_data_file_path)


def list_od_extract_file_names() -> List[str]:
    """Returns a list containing the names of all OD extract files."""
    return [
        x for x in os.listdir(OD_EXTRACTS_FILE_DIR) if x.endswith(".feather")
    ]


def prepare_od_extract_data(od_extract: pd.DataFrame) -> pd.DataFrame:
    """Add additional features and select columns"""
    stopped_vessel_moving_status = ['aground', 'moored', 'at anchor']
    stopped_minimum_speed = 0.5
    # Add binary flag: is the vessel currently moving?
    od_extract['is_stopped'] = od_extract.apply(
        lambda row: (
            1 if row['NavStatus'] in stopped_vessel_moving_status and row['Speed'] <= stopped_minimum_speed
            else 0
        ),
        axis=1
    )
    od_extract['is_moving'] = od_extract['is_stopped'].apply(
        lambda x: 0 if x == 1 else 1
    )
    # Add month number as an alternative (possibly less noisy?) encoding for seasonality effects.
    od_extract['month'] = od_extract['TimePosition'].apply(
        lambda x: x.to_pydatetime().month
    )
    # Subset columns
    od_extract = od_extract[od_extract_selected_columns]

    # Rename columns
    od_extract = od_extract.rename(columns=od_extract_columns_map)

    return od_extract


if __name__ == "__main__":
    main()
