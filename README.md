# ocean-pta-training
> Incremental feature extraction, model training, and scoring for **Ocean PTA**
>
>> **Note:** This process runs locally because the MLOps pipeline and workflow is currently under development.
>> A future story is required to connect this repository with MLOps. With this in mind, azureml.core is part
>> included in `requirements.txt`.

## Setup

### Configuration File

```yaml

```

### Environment Variables

The following environment variables are ***required***:

| VARIABLE                        | FUNCTION                                                    |
|---------------------------------|-------------------------------------------------------------|
| `CONFIG_PATH`                   | Path to the configuration file                              |
| `PATH_TO_PORTS_FILE`            | Local path to the data file `ports_trimmed_modified.csv`    |
| `PATH_TO_VESSEL_MOVEMENTS_DATA` | Path to the data file `vessel_movements_with_hexes.feather` |
| `PATH_TO_OD_FILE`               | Path to the data file `od_routes_v2.csv`                    |
|`PATH_TO_OUTPUT_DIRECTORY`       | Location of the directory where output data will be written. The code will **NOT** create this folder for you. |

The following environment variables are ***optional***:

| VARIABLE           | FUNCTION                       |
|--------------------|--------------------------------|
| `PATH_TO_LOG_FILE` | Path to the configuration file |

### Environment file

Environment variables are specified via a text file, having the default path `./.env`, where `./` is the
project-level working directory. If the environment variable has a different name, or is located somewhere else,
you can give the path to this file as a command line argument. For more on this option, read the Usage section.

### Installation

1. Create a new virtual environment, and activate it.
2. Install requirements
```
pip install -r requirements.txt
```

## Usage

**Extract features and train models locally for the routes specified in your configuration file**

1. Use  the default location for the `.env` file:

```
main.py
```

2. Use a custom location for the `.env` file:

```
main.py /path/to/your/environment/file
```

## Future Notes

* In a future version of this repo, `train.py` will be invoked by the MLOps workflow.
  This script is currently unimplemented.