import json
import logging
from lightgbm import LGBMRegressor
import os
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import pickle
from typing import Any, Dict, List, Tuple
from .config import configs as default_configs

class ModelTrainer(object):
    """
    Train a PTA model and score the predictions in terms of total lead time
    """
    TRAIN_PORTION: float = 2/3
    CV_PORTION: int = 1/4

    # Hyperparameters (currently set at fixed values)
    LEARNING_RATE: float = 0.01
    MAX_ITER_WITHOUT_IMPROVEMENT: int = 100
    MAX_NUM_OF_LEAVES: int = 63
    NUM_OF_TREES: int = 1000

    # Other constants
    PORT_CODE_LENGTH: int = 5
    TRAINING_FILE_EXTENSION: str = ".feather"

    target: str          # Response variable
    features: List[str]  # List of predictor variables

    model_dir: str
    training_jobs: List[Dict]

    def __init__(self, material_root_dir: str):

        self.logger = logging.getLogger(f"{__file__}.{__class__.__name__}")
        self.features = default_configs.get("FEATURES")
        self.target = default_configs.get("TARGET")
        self.set_model_dir(material_root_dir)
        self.set_training_jobs(material_root_dir)

    def set_model_dir(self, material_root_dir: str) -> None:
        """Sets the local file directory for trained models"""
        if os.path.isdir(material_root_dir):
            model_dir = os.path.join(material_root_dir, "models")
            if not os.path.isdir(model_dir):
                os.mkdir(model_dir)

            self.model_dir = model_dir
        else:
            message = "Cannot create the directory for trained models because its parent does not exist."
            self.logger.error(message)
            raise OSError(message)

    def set_training_jobs(self, material_root_dir: str) -> None:
        """Construct a list of training files from the contents of the specified directory"""
        self.training_jobs = []

        if os.path.isdir(material_root_dir):
            train_data_dir = os.path.join(material_root_dir, "od_extracts")
            if not os.path.isdir(train_data_dir):
                message = "Training process will terminate because there is no directory for the training data. You need to run the feature extraction process first."  # noqa
                self.logger.error(message)
                raise OSError(message)

            for file_name in os.listdir(train_data_dir):
                file_path = os.path.join(train_data_dir, file_name)
                if self.is_training_file(file_path):
                    orig, dest = self.get_orig_dest_from_training_file(file_path)
                    self.training_jobs.append({
                        "origin": orig,
                        "destination": dest,
                        "training_file": file_path
                    })
        else:
            message = "Cannot find the directory for model training data because its parent does not exist."
            self.logger.error(message)

    def get_orig_dest_from_training_file(self, file_path: str) -> Tuple[str, str]:
        """Derive origin-destination pair from the encoded file name"""
        code_length: int = self.PORT_CODE_LENGTH
        orig_dest: str = os.path.split(file_path)[-1].replace(self.TRAINING_FILE_EXTENSION, "")
        return orig_dest[:code_length], orig_dest[code_length:]

    def is_training_file(self, file_path: str) -> bool:
        """For now we only check that it ends with .feather"""
        return os.path.isfile(file_path) and file_path.endswith(self.TRAINING_FILE_EXTENSION)

    def train(self) -> None:
        for job in self.training_jobs:
            self.train_model(job)

    def train_model(self, job: Dict) -> None:
        """
        Train a model for one OD-pair. This could be broken into a few different
        steps (e.g. load data, prepare data, fit model, save model)
        """
        orig = job.get("origin")
        dest = job.get("destination")
        data_file = job.get("training_file")

        features = self.features
        target = self.target
        train_portion = self.TRAIN_PORTION
        
        journey_str = f"({orig}-->{dest})"

        self.logger.info(f"Training a PTA model for route: {journey_str} using data file at: {data_file}")
        
        self.logger.info(f"Loading the dataset")
        df = pd.read_feather(data_file)

        """
        NOTE (ASL): Moved Series' type conversion to make it prior to data slitting. This
        simplifies the code and gives me assurance that categories have the same encoding
        across all the splits
        """
        # this one I just moved up in the file (removes a duplicate line of code)
        df['IMO'] = df['IMO'].astype('category')

        # this one I added because model fitting errored out (it wasn't coercing UInt32 to int)
        df['week'] = df['week'].astype(int)
        
        message = f"Splitting the data with {round(train_portion*100, 1)}% of unique route IDs assigned to train"
        self.logger.info(message)
        
        split_routes = np.int32(df.unique_route_ID.nunique()*train_portion)-1
        train_df = df[df.unique_route_ID <= split_routes][features]
        test_df = df[df.unique_route_ID > split_routes][features]

        X_train = train_df[train_df.columns.difference(target)].copy()
        X_test = test_df[train_df.columns.difference(target)].copy()
        y_train = train_df[target].to_numpy().ravel()
        y_test = test_df[target].to_numpy().ravel()

        self.logger.info(f"The training dataset contains {X_train.shape[0]}")
        self.logger.info(f"The test dataset contains {X_test.shape[0]}")

        X_cv_train, X_cv_test, y_cv_train, y_cv_test = train_test_split(
            X_train, y_train,
            test_size=self.CV_PORTION, random_state=42
        )
        # Minimum size of leaf nodes is a function of the size of the training data
        min_data_in_leaf_calc = np.maximum(np.minimum(np.int32(len(X_cv_train) / 1000), 100), 20)

        self.logger.info("Training the model")

        gbm = LGBMRegressor(
            metrics=["mse", 'mae'],
            early_stopping_round=self.MAX_ITER_WITHOUT_IMPROVEMENT,
            min_child_samples=min_data_in_leaf_calc,
            learning_rate=self.LEARNING_RATE,
            n_estimators=self.NUM_OF_TREES,
            num_leaves=self.MAX_NUM_OF_LEAVES
        )
        gbm.fit(
            X_cv_train, y_cv_train,
            eval_set=[(X_cv_test, y_cv_test)],
            verbose=False
        )
        self.logger.info("Model fitting is finished")

        train_elapsed_time = train_df['elapsed_time'].to_numpy().ravel()
        test_elapsed_time = test_df['elapsed_time'].to_numpy().ravel()

        train_metrics = self.evaluate_predictions(
            y=(y_train + train_elapsed_time),
            y_hat=(gbm.predict(X_train) + train_elapsed_time)
        )
        test_metrics = self.evaluate_predictions(
            y=(y_test + test_elapsed_time),
            y_hat=(gbm.predict(X_test) + test_elapsed_time)
        )
        self.log_metrics(journey_str, train_metrics, is_test=False)
        self.log_metrics(journey_str, test_metrics, is_test=True)

        # Save model
        model_file_name = self.get_model_file_name(orig, dest)
        self.save_model_locally(gbm, model_file_name)

    def log_metrics(self, model_name: str, metrics: Dict, is_test: bool) -> None:
        """Log model scoring metrics to INFO level"""
        cohort = "TEST" if is_test else "TRAINING"
        message = f"\nMODEL: \n{model_name}  [{cohort} METRICS]\n{json.dumps(metrics, indent=3)}"
        self.logger.info(message)

    def save_model_locally(self, model_obj: Any, file_name: str) -> None:
        """Save a trained model_locally"""
        file_path = os.path.join(self.model_dir, file_name)

        with open(file_path, "wb") as pickle_file:
            pickle.dump(model_obj, pickle_file)

    @staticmethod
    def get_model_file_name(orig: str, dest: str):
        """Derive a file name for a newly trained model"""
        return f"pta_{orig}{dest}.pickle"

    @staticmethod
    def evaluate_predictions(y, y_hat) -> Dict:
        """
        Compute, log, and return a dict of model evaluation metrics
        """
        return dict(
            positive_bias=np.nanmean(np.maximum((y_hat - y), 0)),
            negative_bias=np.nanmean(np.minimum((y_hat - y), 0)),
            mad=np.nanmean(np.abs(y - y_hat)),
            mse=np.nanmean(np.square(y - y_hat)),
            mape=100. * np.nansum(np.abs(y - y_hat)) / np.nansum(y),
            smape=100. * np.nanmean(np.abs(y - y_hat) / ((np.abs(y) + np.abs(y_hat)) / 2.)),
            md=np.nanmean(y_hat - y),
            mean_y=np.nanmean(y),
            mean_y_hat=np.nanmean(y_hat),
            min_y=np.min(y),
            min_y_hat=np.min(y_hat),
            max_y=np.max(y),
            max_y_hat=np.max(y_hat)
        )
