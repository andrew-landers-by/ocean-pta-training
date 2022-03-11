import os
import logging
import pandas as pd
import pickle
from ocean_pta_training import Environment
from typing import Tuple

logger = logging.getLogger(f"{__name__}")


def main():
    try:
        training_data = add_additional_features()
        print(training_data.info())
    except Exception as e:
        logger.error(f"An unexpected exception occurred: {e}")


def add_additional_features() -> pd.DataFrame:

    od_summary: pd.DataFrame
    while_moving_od_summary: pd.DataFrame

    od_summary, while_moving_od_summary = summarize_combined_od_data()

    # Load training data for ocean journeys
    training_data = load_training_data()

    # Add features based on vessel movements data (by OD)
    training_data = add_od_features(training_data, od_summary)

    # Add features based on vessel movements data (by OD while the vessel is moving).
    training_data = add_od_while_moving_features(training_data, while_moving_od_summary)

    # Add features based on port sequences # combined_port_sequences.csv
    training_data = add_port_sequence_features(training_data)

    return training_data


def add_od_features(training_data: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    # Merge with summary dataframes to construct new data series
    od_moving_portion_series = (
        training_data.merge(summary_df, how="left", on="OD")
        ['moving_portion']
    )
    od_moving_portion_series.index = training_data.index
    training_data['journey_time_moving_pct'] = od_moving_portion_series
    return training_data


def add_od_while_moving_features(training_data: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    while_moving_features_df = (
        training_data.merge(summary_df, how="left", on="OD")
        [['avg_speed', 'median_speed']]
    )
    while_moving_features_df.index = training_data.index
    training_data['median_speed_while_moving'] = while_moving_features_df['median_speed']
    return training_data

def add_port_sequence_features(training_data: pd.DataFrame) -> pd.DataFrame:
    # Load port sequence data summary
    file_path = os.environ.get(Environment.Vars.PATH_TO_COMBINED_PORT_SEQUENCE_DATA)
    port_sequence_df = pd.read_csv(file_path)

    imo_od_summary = imo_od_port_sequence_summary(port_sequence_df)
    od_summary = od_port_sequence_summary(port_sequence_df)

    imo_od_mapped_features = (
        training_data.merge(imo_od_summary, how="left", on=["IMO", "OD"])
        [['n', 'avg_intermediate_ports', 'median_intermediate_ports']]
    )
    imo_od_mapped_features.index = training_data.index

    od_mapped_features = (
        training_data.merge(od_summary, how="left", on=["OD"])
        [['n', 'avg_intermediate_ports', 'median_intermediate_ports']]
    )
    od_mapped_features.index = training_data.index

    def get_mapped_avg_intermediate_ports(idx: int) -> float:
        min_sample_size = 5
        if imo_od_mapped_features['n'].loc[idx] >= min_sample_size:
            return imo_od_mapped_features.loc[idx]
        else:
            return od_mapped_features.loc[idx]

    print("Computing avg_intermediate_ports")
    training_data['avg_intermediate_ports'] = (
        training_data.apply(
            lambda row: get_mapped_avg_intermediate_ports(row.name),
            axis=1
        )
    )
    print(training_data['avg_intermediate_ports'].describe())

    return training_data


def od_port_sequence_summary(port_sequence_df: pd.DataFrame):
    return (
        port_sequence_df
        .groupby('OD')
        .agg(
            n=('num_intermediate_ports', 'count'),
            avg_intermediate_ports=('num_intermediate_ports', 'mean'),
            median_intermediate_ports=('num_intermediate_ports', 'median')
        )
    )


def imo_od_port_sequence_summary(port_sequence_df: pd.DataFrame):
    return (
        port_sequence_df
        .groupby(['IMO', 'OD'])
        .agg(
            n=('num_intermediate_ports', 'count'),
            avg_intermediate_ports=('num_intermediate_ports', 'mean'),
            median_intermediate_ports=('num_intermediate_ports', 'median')
        )
    )


def summarize_combined_od_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    combined_df: pd.DataFrame = load_combined_data()
    by_od_summary = summarize_by_od(combined_df)
    by_od_while_moving_summary = summarize_by_od_while_moving(combined_df)
    return by_od_summary, by_od_while_moving_summary

def summarize_by_od(combined_df: pd.DataFrame) -> pd.DataFrame:
    od_features_df = (
        combined_df
        .groupby('OD')
        .agg(
            moving_portion=('is_moving', 'mean')
        )
    )
    return od_features_df

def summarize_by_od_while_moving(combined_df: pd.DataFrame) -> pd.DataFrame:
    od_moving_features = (
        combined_df
        [combined_df['is_moving'] == 1]
        .groupby('OD')
        .agg(
            avg_speed=('speed', 'mean'),
            median_speed=('speed', 'median')
        )
    )
    return od_moving_features


def load_training_data() -> pd.DataFrame:
    file_path = os.environ.get(Environment.Vars.PATH_TO_LOCAL_TRAINING_DATA)
    try:
        with open(file_path, 'rb') as pickle_file:
            return pickle.load(pickle_file)
    except Exception as e:
        logger.error(f"Load local training data failed due to {type(e).__name__}: {e}")


def load_combined_data() -> pd.DataFrame:
    file_path = os.environ.get(Environment.Vars.PATH_TO_COMBINED_OD_DATA)
    try:
        return pd.read_feather(file_path)
    except Exception as e:
        logger.error(f"Load combined OD data failed due to {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
