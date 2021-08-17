
from azureml.core import Workspace, Datastore, Dataset, Run
from azure.storage.filedatalake import DataLakeServiceClient
import os

from main import main as train_main


def download_data(run):
    workspace = run.experiment.workspace

    datastore = Datastore.get(workspace, datastore_name='datalakesprintdemo')
    datastore_paths = [
        (datastore, 'datasets/training_data/ports_trimmed_modified.csv'),
        (datastore, 'datasets/training_data/vessel_movements_with_hexes.feather'),
        (datastore, 'datasets/training_data/od_routes_v2.csv')
    ]
    dataset = Dataset.File.from_files(path=datastore_paths)

    try:
        dataset.download(target_path='./data/', overwrite=False)
    except Exception as e:
        print(f'did not download: {e}')


def upload_models(run):
    storage_account_name = 'ptatempstorage'
    storage_account_key = 'vzzqKc0VpoSl2rp3Jd5XCJgBrL27jEFT3yEw+OUEUw4Bj1iMzM/wN+kk6Dzj7C05FwfszGK17c2R3bQDfMvJyA=='
    container_name = 'mlops-workflows'
    model_directory = f'pta-models/{run.id}'
    local_model_path = 'output/models'

    service_client = DataLakeServiceClient(
        account_url=f'https://{storage_account_name}.dfs.core.windows.net',
        credential=storage_account_key
    )

    file_system_client = service_client.get_file_system_client(file_system=container_name)
    directory_client = file_system_client.get_directory_client(model_directory)

    for f in [f for f in os.listdir(local_model_path)]:
        print(f'uploading: {f}')

        local_file = open(os.path.join(local_model_path, f), 'rb')
        file_contents = local_file.read()

        file_client = directory_client.get_file_client(f)
        file_client.upload_data(file_contents, overwrite=True)


def train():
    run = Run.get_context()

    download_data(run)

    train_main()

    upload_models(run)



if __name__ == "__main__":
    train()
