import logging
import os

import pandas as pd
from ocean_pta_training import Environment
from ocean_pta_training.geojson_inference import calculate_shortest_path

BATCH_SIZE = 10000
ROWS_LIMIT = None
POINTS_OF_INTEREST = [
    'babelmandeb', 'bering', 'corinth', 'dover', 'gibraltar', 'kiel', 'magellan',
    'malacca', 'northeast', 'northwest', 'panama', 'suez'
]
FEATURE_COLUMNS = ['ocean_distance', 'source_to_network_dist', 'network_to_dest_dist']
FEATURE_COLUMNS = FEATURE_COLUMNS + POINTS_OF_INTEREST

logger = logging.getLogger(f"{__name__}")


def main():
    try:
        labeling_df = load_unlabeled_data()

        if ROWS_LIMIT:
            labeling_df = labeling_df[:ROWS_LIMIT]

        logger.info(f"\n{labeling_df.info()}")
        prepare_unlabeled_data(labeling_df)
        label(labeling_df)
        logger.info(f"\n{labeling_df}")
        save_labeled_data(labeling_df)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        save_labeled_data(labeling_df)

def label(labeling_df: pd.DataFrame):
    rows_total = len(labeling_df[labeling_df['remaining_distance_flag']].index)
    rows_complete = 0
    rows_failed = 0
    batch_df = get_batch_df(labeling_df)
    while not is_labeling_finished(batch_df):
        try:
            logger.info(f"labeling a partition of {len(batch_df.index)} rows")
            label_one_batch(batch_df)
            logger.info("inserting features...")
            for col_name in FEATURE_COLUMNS:
                labeling_df.loc[batch_df.index, col_name] = batch_df[col_name]
            labeling_df.loc[batch_df.index, 'labeled'] = True
            rows_complete += len(batch_df.index)

        except Exception as e:
            logger.info(
                f"Unexpected error occurred while attempting to label the data: {e}."
                f"The rows in this batch will be flagged as 'failed_job'."
            )
            labeling_df.loc[batch_df.index, 'failed_job'] = True
            rows_failed += len(batch_df.index)

        logger.info(f"{rows_complete} rows labeled, {rows_failed} failed out of {rows_total} total")

        batch_df = get_batch_df(labeling_df)

def get_batch_df(labeling_df: pd.DataFrame):
    batch_df = labeling_df[
        (labeling_df['remaining_distance_flag']) &
        (~labeling_df['labeled']) &
        (~labeling_df['failed_job'])
    ]
    return batch_df[:BATCH_SIZE]

def is_labeling_finished(batch_df: pd.DataFrame) -> bool:
    return len(batch_df.index) == 0

def label_one_batch(batch_df: pd.DataFrame) -> pd.DataFrame:
    results = calculate_shortest_path(batch_df)

    # Populate ocean distance, source to network distance, and network to destination distance
    batch_df.loc[batch_df.index, 'ocean_distance'] = results.get('ocean_distance')
    batch_df.loc[batch_df.index, 'source_to_network_dist'] = results.get('distance_from_source_to_network')
    batch_df.loc[batch_df.index, 'network_to_dest_dist'] = results.get('distance_to_dest_from_network')

    # Populate points of interest flags
    points_of_interest = results.get('points_of_interest')
    for i, idx in enumerate(batch_df.index):
        batch_df.loc[idx, POINTS_OF_INTEREST] = points_of_interest[i]

    return batch_df

def prepare_unlabeled_data(labeling_df: pd.DataFrame):
    """TODO"""

    labeling_df.rename(
        columns={'latitude': 'olat', 'longitude': 'olon'},
        inplace=True
    )
    mark_destination_latlon(labeling_df)
    labeling_df['labeled'] = False
    labeling_df['failed_job'] = False
    labeling_df['ocean_distance'] = None
    labeling_df['source_to_network_dist'] = None
    labeling_df['network_to_dest_dist'] = None

    for i in range(len(POINTS_OF_INTEREST)):
        col_name = POINTS_OF_INTEREST[i]
        labeling_df[col_name] = pd.Series(
            index=labeling_df.index, dtype='int32'
        )

def mark_destination_latlon(labeling_df: pd.DataFrame):
    logger.info("...attaching destination lat/lon to unlabeled data...")
    ports_data = load_ports_data()
    df = labeling_df['OD'].str.split('-', expand=True)
    df.columns = ['origin', 'destination']

    # Merge with ports_data to get destination lat/lon
    df = df.merge(
        ports_data,
        how='left', left_on='destination', right_on='locode',
    )
    df.index = labeling_df.index

    # Attach columns
    labeling_df['dlat'] = df['lat']
    labeling_df['dlon'] = df['lon']

def load_ports_data() -> pd.DataFrame:
    ports_data_file_path = os.environ.get(Environment.Vars.PATH_TO_PORTS_FILE)
    return pd.read_csv(ports_data_file_path)

def load_unlabeled_data() -> pd.DataFrame:
    file_path = os.environ.get(Environment.Vars.PATH_TO_SEAROUTES_UNLABELED_DATA)
    df = pd.read_feather(file_path)
    df['labeled'] = False
    return df

def save_labeled_data(labeling_df: pd.DataFrame):
    file_path = os.environ.get(Environment.Vars.PATH_TO_SEAROUTES_LABELED_DATA)
    logger.info(f"Saving labeled data to file: {file_path}")
    labeling_df.to_feather(file_path)


if __name__ == "__main__":
    main()
