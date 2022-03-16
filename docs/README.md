# ocean-pta-training
> Feature extraction, model training, and scoring for `Ocean PTA`:
>
> 1. `LightGBM` regressor for predicting remaining lead time with an origin-destination (OD) specific model.
> 2. `[In-Progress]` OD-agnostic remaining lead time prediction dervived from a combined dataset.
>

## **1.** Setup

### **1.1.** Config File

The YAML config file specifies the set of routes that will be processed. For each item, a training file and a trained model
will be generated upon execution of `main.py`.

```yaml
JOBS:
  CNLYG-CNSZX:
    destination: CNSZX
    origin: CNLYG
  CNLYG-KRINC:
    destination: KRINC
    origin: CNLYG
  CNQDG-PHMNL:
    destination: PHMNL
    origin: CNQDG
  DEWVN-ESALG:
    destination: ESALG
    origin: DEWVN
  IDJKT-USSAV:
    destination: USSAV
    origin: IDJKT
```

### **1.2.** Environment Variables

The following environment variables are ***required***:

| VARIABLE                              | EXAMPLE | FUNCTION                                          |
|---------------------------------------|---------|---------------------------------------------------|
| `PATH_TO_NEW_OD_REQUIREMENT_CSV_FILE` | N/A     | Local path to the CSV file specifying which origin-destination route training to attempt with `main.py`. |
| `CONFIG_PATH`                         | N/A     | Local path to the configuration file specifying OD-specific training jobs. |
| `PATH_TO_PORTS_FILE`                  | N/A     | Local path to the data file `ports_trimmed_modified.csv`    |
| `PATH_TO_VESSEL_MOVEMENTS_DATA`       | N/A     | Local path to the data file having vessel movements history |
| `PATH_TO_OD_FILE`                     | N/A     | Local path to the data file having global ports and some of their attributes |
| `PATH_TO_OUTPUT_DIRECTORY`            | N/A     | Local path to the directory where all output data will be written. If not provided, this will be created as `./output/`, from the project level working directory. |
| `PATH_TO_LOG_FILE_DIRECTORY`          | N/A     | Local path to the directory where python logs will be written to. |
| `PATH_TO_COMBINED_OD_DATA`            | N/A     | Local path to output file: combined OD dataset. |
| `PATH_TO_COMBINED_PORT_SEQUENCE_DATA` | N/A     | Feature extraction from vessel movements will summarize the routes in a CSV file written to this local path. |
| `PATH_TO_GEOJSON_UNLABELED_DATA`      | N/A     | Local path to output file: unlabeled dataset for geojson inference |
| `PATH_TO_GEOJSON_LABELED_DATA`        | N/A     | Local path to output file: labeled dataset from geojson inference |
| `PATH_TO_LOCAL_TRAINING_DATA`         | N/A     | Local path where OD-agnostic training data will be written |
| `ODBC_DRIVER`                         | {ODBC Driver 17 for SQL Server} | ODBC driver (for Synapse database connection) |
| `SYNAPSE_SERVER`                      | lana-sqlserver-dev-01.database.windows.net | Synapse server host |
| `SYNAPSE_DATABASE`                    | lana-synapse-dev-01 | Synapse database |
| `SYNAPSE_UID`                         | N/A     | Synapse database user ID |
| `SYNAPSE_PASSWORD`                    | N/A     | Synapse database password |
| `SYNAPSE_SCHEMA`                      | ocean_vessel_movement   | Database schema for Ocean PTA training data |
| `OCEAN_JOURNEY_FEATURES_TABLE`        | OCEAN_JOURNEY_FEATURES  | Name of Synapse table having Ocean PTA training features (OD-agnostic) |
| `OCEAN_JOURNEY_RESPONSE_TABLE`        | OCEAN_JOURNEY_RESPONSE  | Name of Synapse table having Ocean PTA training response variable (OD-agnostic) |
| `OCEAN_JOURNEY_DATA_TABLE`            | OCEAN_JOURNEYS          | Name of Synapse table having joined training data (raw features and response) |
| `OCEAN_PORTS_TABLE`                   | OCEAN_PORTS             | Name of Synapse table describing ocean ports |
| `OCEAN_VESSELS_TABLE`                 | OCEAN_VESSELS           | Name of Synapse table describing ocean vessels |
| `OCEAN_JOURNEY_CHUNK_CSV_FILE_DIR`    | N/A     | Features and response records are written here locally, then deleted after uploading to blob storage |
| `BLOB_SERVICE_URL`                    | https://oceanptastorelanadev01.blob.core.windows.net | URL for the blob storage account |
| `BLOB_SERVICE_CONNECTION_STRING`      | N/A     | Connection string for the blob storage account |
| `BLOB_SERVICE_ACCESS_KEY`             | N/A     | Access code secret for the blob storage account |
| `BLOB_SERVICE_CONTAINER_NAME`         | file-share | Blob container used by this project|
| `OCEAN_JOURNEY_FEATURES_BLOB_SUBDIR`  | ocean_journeys/features | Blob container subdirectory where CSV files (features) are written |
| `OCEAN_JOURNEY_RESPONSE_BLOB_SUBDIR`  | ocean_journeys/response | Blob container subdirectory where CSV files (response) are written |


### **1.3.** Environment file

Environment variables are specified via a text file, having the path `./.env`, where `./` is the
project-level working directory. Login credentials and local file paths are not checked into the Git repository.
Instead, either a placeholder or a missing value are checked in -- the user will clone the repository and then
provide the correct values in `.env`.

### **1.4.** Installation

1. Create a new virtual environment, and activate it.
   
```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
```

2. Install requirements

```
pip install -r requirements.txt
```

### **1.5.** Input file: CSV for target OD routes

You can create a config file, specifying the set of OD routes for which to extract data and train models, by executing 
the script `00_make_config_file_from_csv.py`. First, specify the routes in the file mapped to environment variable
`PATH_TO_NEW_OD_REQUIREMENT_CSV_FILE`, as a CSV file with the following format:

| origin_port | destination_port |
| ------------|------------------|
| KRBUK       | CNQDG            |
| KRBUK       | CNSHG            |
| KRBUK       | CNSZX            |
| KRBUK       | SGSIN            |
| KRBUK       | MYOKG            |

Then, invoke the following script to read the CSV file into a new YAML configuration file. The resulting YAML file
will match the format shown in section 1.1, above.

```
python 00_make_config_file_from_csv.py
```

## 2. Usage

### 2.1. **OD-based ML models**

Extract features, train, and score models locally for the routes specified in your configuration file:

```
python main.py
```

Read in OD route requirements from CSV, then extract training data, then train and score models:

```
python 00_make_config_file_from_csv.py
python main.py
```

Extract OD training data, but do not train/score the models:

```
01_extract_routes_with_local_configs.py
```

Train OD models from pre-existing training data `.feather` files (previously generated):

```
02_train_od_models.py
```

### 2.2. [In-Progress] **OD-agnostic ML models**

**2.2.1.** Build a combined dataset from all ODs in the training scope (from `$CONFIG_PATH`)

```
python 00_make_config_file_from_csv.py
python 01_extract_routes_with_local_configs.py
python 03_build_combined_dataset.py
```

**2.2.2.** Perform geojson inference on sampled events to derive features such as estimated remaining distance and presence of choke points on that estimated path.
    - **Predecessor:** `03_build_combined_dataset.py`
```
python 04_extract_unlabeled_data_for_geojson_inference.py
python 05_label_data_with_geojson_inference.py
```

**2.2.3.** Upload features (post-labeling) and prediction target (response) to cloud storage:
    - **Predecessor:** `05_label_data_with_geojson_inference.py`
```
python 06_upload_features_to_cloud_storage.py
python 07_upload_response_to_cloud_storage.py
```

**2.2.4.** Use previously-uploaded CSV files to create and populate Synapse tables, then assemble combined (raw) training data
    - **Predecessor:** `07_upload_response_to_cloud_storage.py`
```
python 08a_create_synapse_tables.py
python 08b_populate_synapse_tables.py
python 08c_create_combined_training_data.py
```

**2.2.5.** Clean data and add derived features locally:
    - **Predecessor:** `08a_create_synapse_tables.py`
    - **Predecessor:** `08b_populate_synapse_tables.py`
    - **Predecessor:** `08c_create_combined_training_data.py`
```
python 09_download_all_training_data.py
python 10_remove_anomaly_journeys.py
python 11_add_additional_features.py
```