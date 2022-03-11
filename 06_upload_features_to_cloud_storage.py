import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from ocean_pta_training import Environment
from typing import List

DROPPED_FIELDS = ['remaining_distance_flag', 'labeled', 'failed_job']
RANDOM_SEED = 12345
SAMPLE_SIZE = None
CSV_CHUNK_SIZE = int(1e5)

logger = logging.getLogger(f"{__name__}")

def main():
    try:
        features_df = load_data()
        sort_data(features_df)
        logger.info(f"Samples:\n{features_df}\n{features_df.info()}")
        upload_csv_chunks(features_df)
    except Exception as e:
        logger.error(f"An unexpected exception occurred: {e}")


def upload_csv_chunks(df: pd.DataFrame):
    """
    TODO
    """
    csv_file_names = write_csv_chunks(df)
    copy_csv_chunks_to_blob_service(csv_file_names)
    delete_local_csv_chunks(csv_file_names)

def copy_csv_chunks_to_blob_service(csv_file_names: List[str]) -> None:
    """
    TODO
    """
    blob_container_name = os.environ.get(Environment.Vars.BLOB_SERVICE_CONTAINER_NAME)
    blob_subdir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_BLOB_SUBDIR)
    csv_file_dir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_CHUNK_CSV_FILE_DIR)
    with blob_service_client() as service_client:
        container_client: ContainerClient = service_client.get_container_client(blob_container_name)
        for file_name in csv_file_names:
            file_path = os.path.join(csv_file_dir, file_name)
            blob_key = f"{blob_subdir}/{file_name}"
            with open(file_path, 'rb') as file_stream:
                logger.info(f"Uploading data from file at {file_path} to blob: {blob_key}")
                container_client.upload_blob(name=blob_key, data=file_stream, overwrite=True)


def write_csv_chunks(df: pd.DataFrame) -> List[str]:
    """
    TODO
    """
    chunk_length = CSV_CHUNK_SIZE
    chunks_processed = 0
    csv_file_dir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_CHUNK_CSV_FILE_DIR)
    csv_file_name_root = "ocean_journey_features"

    def csv_file_name(sequence: int) -> str:
        if 0 <= sequence <= 9:
            return f"{csv_file_name_root}_000{sequence}.csv"
        elif 10 <= sequence <= 99:
            return f"{csv_file_name_root}_00{sequence}.csv"
        elif 100 <= sequence <= 999:
            return f"{csv_file_name_root}_0{sequence}.csv"
        else:
            return f"{csv_file_name_root}_{sequence}.csv"

    chunk_start_idx = 0
    chunk_stop_idx = chunk_start_idx + chunk_length
    chunk_df = df.iloc[chunk_start_idx:chunk_stop_idx]
    csv_file_names = []
    while len(chunk_df.index) > 0:
        new_csv_file_name = csv_file_name(chunks_processed + 1)
        csv_file_path = os.path.join(csv_file_dir, new_csv_file_name)
        logger.info(f"Writing a partition of size {chunk_length} to path: {csv_file_path}")
        chunk_df.to_csv(csv_file_path, index=False)

        csv_file_names.append(new_csv_file_name)
        chunks_processed += 1
        chunk_start_idx += chunk_length
        chunk_stop_idx = chunk_start_idx + chunk_length
        chunk_df = df.iloc[chunk_start_idx:chunk_stop_idx]

    return csv_file_names


def delete_local_csv_chunks(csv_file_names: List[str]):
    """
    TODO
    """
    csv_file_dir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_CHUNK_CSV_FILE_DIR)
    logger.info(f"Deleting local data files in directory: {csv_file_dir}")
    for file_name in csv_file_names:
        try:
            file_path = os.path.join(csv_file_dir, file_name)
            os.remove(file_path)
        except Exception as e:
            logger.error(
                f"Delete csv file {file_path} failed due to {type(e).__name__}. Error: {e}"  # noqa
            )


def load_data() -> pd.DataFrame:
    """
    Draw random samples from labeled data
    """
    logger.info("Loading labeled data")
    all_data = load_labeled_data()

    selected_fields = list(all_data.columns)
    for field in DROPPED_FIELDS:
        selected_fields.remove(field)

    candidate_df = all_data[
        (all_data['labeled']) &
        (all_data['ocean_distance'] >= 0)
    ]
    return (
        candidate_df
        .sample(
            n=SAMPLE_SIZE if SAMPLE_SIZE else len(candidate_df.index),
            replace=False,
            random_state=RANDOM_SEED
        )
        [selected_fields]
    )

def sort_data(df: pd.DataFrame):
    """
    TODO
    """
    (
        df.sort_values(
            by=['IMO', 'OD', 'unique_route_id', 'elapsed_time'],
            ascending=[True, True, True, True],
            inplace=True
        )
    )


def load_labeled_data():
    """
    TODO
    """
    data_file = os.environ.get(Environment.Vars.PATH_TO_SEAROUTES_LABELED_DATA)
    logger.debug(f"Loading labeled data from file: {data_file}")
    return pd.read_feather(data_file)

def blob_service_client() -> BlobServiceClient:
    """
    TODO
    """
    conn_str = os.environ.get(Environment.Vars.BLOB_SERVICE_CONNECTION_STRING)
    return BlobServiceClient.from_connection_string(conn_str)


if __name__ == "__main__":
    main()
