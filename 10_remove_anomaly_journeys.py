"""
This module will save a local copy of the training. This way,
we will not need to download the data from Synapse each time
"""
import os
import logging
import pandas as pd
import pickle
from ocean_pta_training import Environment
from typing import Dict, List

logger = logging.getLogger(f"{__name__}")

REMAINING_LEAD_TIME_CUTOFF = 80


def main():
    try:
        all_data_df = load()
        sort_data(all_data_df)
        all_data_df = all_data_df
        all_data_df = exclude_outlier_remaining_lead_time(all_data_df)
        anomalies = flag_anomaly_journeys(all_data_df)
        logger.info(f"[ANOMALY_JOURNEYS] {anomalies}")
        all_data_df = exclude_anomalies(all_data_df, anomalies)
        logger.info(f"[CLEANED_DATASET]\n{all_data_df}")
        save(all_data_df)
    except Exception as e:
        logger.error(f"An unexpected exception occurred: {e}")


def exclude_anomalies(journeys_df: pd.DataFrame, anomalies: List[Dict]):
    """
    Remove journeys having anomalous patterns in the estimated remaining distance
    (e.g. a very large jump in remaining distance after only a few hours timedelta
    """
    trim_record = pd.Series(False, journeys_df.index)
    for anomaly in anomalies:
        anomaly_df = journeys_df[
            (journeys_df['IMO'] == anomaly.get('IMO')) &
            (journeys_df['OD'] == anomaly.get('OD')) &
            (journeys_df['unique_route_id'] == anomaly.get('unique_route_id'))
        ]
        trim_record.loc[anomaly_df.index] = True

    return (
        journeys_df[~trim_record]
        .reset_index(drop=True)
    )


def exclude_outlier_remaining_lead_time(journeys_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a hard cutoff on the observed value of remaining_lead_time
    """
    cutoff = REMAINING_LEAD_TIME_CUTOFF
    return (
        journeys_df
        [journeys_df['remaining_lead_time'] <= cutoff]
        .reset_index(drop=True)
    )


def is_distance_step_anomalous(this_idx, this_imo, this_route_id, this_ocean_distance,
                               last_imo, last_route_id, last_ocean_distance
                               ) -> bool:
    """
    Return True if the change between two adjacent values of estimated ocean distance
    is unrealistically large; otherwise return False
    """
    distance_threshold = 5000

    if this_idx == 0:
        # Boundary condition: row 0 has no previous record
        return False
    elif this_imo != last_imo or this_route_id != last_route_id:
        return False
    elif abs(this_ocean_distance - last_ocean_distance) > distance_threshold:
        return True
    else:
        return False


def flag_anomaly_journeys(journeys_df) -> List:
    """
    Analyze each unique journey and flag the records where remaining
    distance is an unrealistic jump compared to the previous values.
    """
    distance_threshold = 2500
    time_delta_threshold = 0.5
    sort_data(journeys_df)

    # We will reference columns by their index (for speed)
    elapsed_time_col_idx = 9999
    imo_col_idx = 9999
    od_col_idx = 9999
    route_id_col_idx = 9999
    ocean_distance_col_idx = 9999
    for idx, col_name in enumerate(journeys_df.columns):
        if col_name == 'IMO':
            imo_col_idx = idx
        elif col_name == 'OD':
            od_col_idx = idx
        elif col_name == 'unique_route_id':
            route_id_col_idx = idx
        elif col_name == 'elapsed_time':
            elapsed_time_col_idx = idx
        elif col_name == 'ocean_distance':
            ocean_distance_col_idx = idx

    journeys_df['is_invalid_jump'] = False
    is_invalid_jump_col_idx = len(journeys_df.columns) - 1

    imo = journeys_df.iloc[0, imo_col_idx]
    od = journeys_df.iloc[0, od_col_idx]
    unique_route_id = journeys_df.iloc[0, route_id_col_idx]
    for idx in range(len(journeys_df.index)):
        if idx == 0:
            continue
        elif (journeys_df.iloc[idx, imo_col_idx] != imo or
              journeys_df.iloc[idx, od_col_idx] != od or
              journeys_df.iloc[idx, route_id_col_idx] != unique_route_id):
            imo = journeys_df.iloc[idx, imo_col_idx]
            od = journeys_df.iloc[idx, od_col_idx]
            unique_route_id = journeys_df.iloc[idx, route_id_col_idx]
        else:
            elapsed_time_delta = (
                journeys_df.iloc[idx, elapsed_time_col_idx] -
                journeys_df.iloc[idx-1, elapsed_time_col_idx]
            )
            this_ocean_dist = journeys_df.iloc[idx, ocean_distance_col_idx]
            last_ocean_dist = journeys_df.iloc[idx - 1, ocean_distance_col_idx]

            if (
                    elapsed_time_delta <= time_delta_threshold and
                    abs(this_ocean_dist - last_ocean_dist) > distance_threshold
            ):
                journeys_df.iloc[idx, is_invalid_jump_col_idx] = True

    journeys_df.to_csv("./journeys_df.csv", index=False)

    anomaly_journeys = (
        journeys_df[(journeys_df['is_invalid_jump'])]
        [['IMO', 'OD', 'unique_route_id']]
        .drop_duplicates()
        .reset_index(drop=True)
        .to_dict(orient='records')
    )
    logger.info(f"Anomalous journeys: {len(anomaly_journeys)}")
    logger.info(f"Total journeys: {len(journeys_df[['IMO', 'OD', 'unique_route_id']].drop_duplicates())}")
    logger.info(f"Flagged {len(anomaly_journeys)} anomalous journeys")
    return anomaly_journeys


def sort_data(df: pd.DataFrame):
    """Sort the dataframe by IMO, OD, unique route, and elapsed tim (required)"""
    df.sort_values(
        by=['IMO', 'OD', 'unique_route_id', 'elapsed_time'],
        inplace=True
    )


def load() -> pd.DataFrame:
    """Load the dataset (local)"""
    local_data_path = os.environ.get(Environment.Vars.PATH_TO_LOCAL_TRAINING_DATA)
    logger.info(f"Loading ocean journeys dataset locally from file: {local_data_path}")
    with open(local_data_path, 'rb') as pickle_file:
        df: pd.DataFrame = pickle.load(pickle_file)
        return df


def save(df: pd.DataFrame):
    """Save the dataset after it has been processed."""
    local_data_path = os.environ.get(Environment.Vars.PATH_TO_LOCAL_TRAINING_DATA)
    logger.info(f"Saving ocean journeys dataset locally as file: {local_data_path}")
    with open(local_data_path, 'wb') as pickle_file:
        pickle.dump(df, pickle_file)
    logger.info(f"Saving ocean journeys dataset to file: {local_data_path}")


if __name__ == "__main__":
    main()
