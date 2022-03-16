"""
This module will save a local copy of the training. This way,
we will not need to download the data from Synapse each time
"""
import os
import logging
import pandas as pd
import pickle
from ocean_pta_training import Environment
from ocean_pta_training.utilities import pyodbc_connect


logger = logging.getLogger(f"{__name__}")

def main():
    try:
        all_data = load()
        save(all_data)
    except Exception as e:
        logger.error(f"An unexpected exception occurred: {e}")


def save(df: pd.DataFrame):
    """
    Saves the combined training data locally to a path specified by
    environment variable PATH_TO_LOCAL_TRAINING_DATA
    """
    local_data_path = os.environ.get(Environment.Vars.PATH_TO_LOCAL_TRAINING_DATA)
    logger.info(f"Saving ocean journeys dataset locally as file: {local_data_path}")
    with open(local_data_path, 'wb') as pickle_file:
        pickle.dump(df, pickle_file)
    logger.info(f"Saving ocean journeys dataset to file: ")

def load() -> pd.DataFrame:
    """
    Draw random samples from labeled data
    """
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_DATA_TABLE)
    sql = f"""
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY IMO, OD, unique_route_id ORDER BY elapsed_time) as journey_obs
    FROM {schema}.{table}
    """
    logger.info(f"Downloading ocean journeys dataset with sql statement:\n\t{sql}")

    with pyodbc_connect() as conn:
        df: pd.DataFrame = pd.read_sql(
            con=conn,
            sql=sql
        )
        logger.info(f"Dataset:\n{df}")
        return df


if __name__ == "__main__":
    main()
