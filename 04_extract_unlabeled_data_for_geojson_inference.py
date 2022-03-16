import logging
import pandas as pd
import os
from ocean_pta_training import Environment

logger = logging.getLogger(f"{__name__}")

HOURS_BETWEEN_REMAINING_DISTANCE_LABELS = 5


def main():
    logger.info("Extracting a dataset to be labeled with remaining distance to destination port...")
    logger.info("...loading combined OD data (unfiltered)...")
    combined_df: pd.DataFrame = read_in_combined_data()
    logger.info(f"...combined dataset:\n{combined_df}\n")
    logger.info(f"...total records: {len(combined_df.index)}")
    logger.info(f"...unique timestamps: {len(combined_df['time_position'].unique())}")
    logger.info(f"...unique IMOs: {len(combined_df['IMO'].unique())}")
    searoutes_jobs: pd.DataFrame = (
        combined_df
        [combined_df['is_moving'] == 1]
        [['IMO', 'OD', 'unique_route_id', 'time_position', 'elapsed_time', 'latitude', 'longitude']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info(f"...unique tuples (IMO, OD, timestamp) when vessel is moving: {len(searoutes_jobs.index)}")
    logger.info(f"...marking records that require remaining distance labels...")
    flag_for_remaining_distance_label(searoutes_jobs)
    logger.info(f"...{searoutes_jobs['remaining_distance_flag'].describe()}")
    output_file_path = os.environ.get(Environment.Vars.PATH_TO_GEOJSON_UNLABELED_DATA)
    logger.info(f"...exporting unlabeled dataset to {output_file_path}")
    searoutes_jobs.to_feather(output_file_path)

def flag_for_remaining_distance_label(movements_df: pd.DataFrame):
    """
    Flag a subset of the data to be processed by geojson inference to calculate
    the shortest ocean route and the point-of-interest flags associated with that route.
    """
    remaining_distance_flag = pd.Series(False, index=movements_df.index)
    remaining_distance_flag.loc[0] = True
    elapsed_time = movements_df['elapsed_time']
    imo = movements_df['IMO']
    od = movements_df['OD']
    route_id = movements_df['unique_route_id']
    this_imo = imo.loc[0]
    this_od = od.loc[0]
    this_route_id = route_id.loc[0]
    days_between = HOURS_BETWEEN_REMAINING_DISTANCE_LABELS/24
    ref_time = 0
    for i in range(len(movements_df.index)):
        if imo.loc[i] != this_imo or od.loc[i] != this_od or route_id.loc[i] != this_route_id:
            remaining_distance_flag.loc[i] = True
            this_imo = imo.loc[i]
            this_od = od.loc[i]
            this_route_id = route_id.loc[i]
            ref_time = elapsed_time.loc[i]
        elif elapsed_time.loc[i] - ref_time >= days_between:
            remaining_distance_flag.loc[i] = True
            this_imo = imo.loc[i]
            this_od = od.loc[i]
            this_route_id = route_id.loc[i]
            ref_time = elapsed_time.loc[i]

    movements_df['remaining_distance_flag'] = remaining_distance_flag

def calculate_time_delta(row, movements_df):
    """
    Calculate the time delta since the last observation,
    at a single row of DataFrame movements_df
    """
    if row.name == 0 or row['elapsed_time'] <= 0:
        return 0
    elif row['elapsed_time'] > 0:
        return row['elapsed_time'] - movements_df.loc[row.name - 1, 'elapsed_time']

def read_in_combined_data():
    """Read in combined OD dataset from a local .feather file"""
    feather_file_path = os.environ.get(Environment.Vars.PATH_TO_COMBINED_OD_DATA)
    try:
        with open(feather_file_path, 'rb') as feather_file:
            return pd.read_feather(feather_file)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
