import pandas as pd
import os
import numpy as np
from lightgbm import LGBMRegressor
from sklearn.model_selection import train_test_split
from skl2onnx import update_registered_converter
from skl2onnx.common.shape_calculator import calculate_linear_regressor_output_shapes
from onnxmltools.convert.lightgbm.operator_converters.LightGbm import convert_lightgbm
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from pipeline_saver import PipelineSaver
import time
import sys
import pickle
import warnings
warnings.filterwarnings('ignore')

#accuracy function
def eval_results(y,yhat_mean):
    positive_bias = np.nanmean(np.maximum((yhat_mean - y),0))
    print('BIAS+: {}'.format(positive_bias))
    negative_bias = np.nanmean(np.minimum((yhat_mean - y),0))
    print('BIAS-: {}'.format(negative_bias))
    mad = np.nanmean(np.abs(y - yhat_mean))
    print('MAD: {}'.format(mad))
    mse = np.nanmean(np.square(y - yhat_mean))
    print('MSE: {}'.format(mse))
    mape = 100. * np.nansum(np.abs(y - yhat_mean)) / np.nansum(y)
    print('MAPE: {}'.format(mape))
    smape = 100. * np.nanmean(np.abs(y - yhat_mean) / ((np.abs(y) + np.abs(yhat_mean)) / 2.))
    print('SMAPE: {}'.format(smape))
    md = np.nanmean(yhat_mean-y)
    print('MD: {}'.format(md))
    mean_y = np.nanmean(y)
    print('mean(y): {}'.format(mean_y))
    mean_yhat = np.nanmean(yhat_mean)
    print('mean(y_hat): {}'.format(mean_yhat))
    min_y = np.min(y)
    print('min(y): {}'.format(min_y))
    min_yhat = np.min(yhat_mean)
    print('min(y): {}'.format(min_yhat))
    max_y = np.max(y)
    print('max(y): {}'.format(max_y))
    max_yhat = np.max(yhat_mean)
    print('max(y): {}'.format(max_yhat))


def make_onnx_models(orig,dest):
    extract_file = os.getcwd() + '/od_extracts/'+orig + dest + '.feather'
    df = pd.read_feather(extract_file)
    split_routes = np.int32(df.unique_route_ID.nunique()*2/3)-1
    train_df = df[df.unique_route_ID <= split_routes][features]
    test_df = df[df.unique_route_ID > split_routes][features]
    X_train = train_df[train_df.columns.difference(target)].copy()
    X_train['IMO'] = X_train['IMO'].astype('category')
    y_train = train_df[target].to_numpy().ravel()
    X_test =  test_df[train_df.columns.difference(target)].copy()
    X_test['IMO'] = X_test['IMO'].astype('category')
    print('Features used are', X_train.columns)
    print(X_train.info())
    print(X_test.info())
    save_path = os.getcwd() + '/test_extracts/'+orig + dest + '.feather'
    X_test.reset_index(drop=True).to_feather(save_path)
    y_test =  test_df[target].to_numpy().ravel()
    train_elapsed_time = train_df['elapsed_time'].to_numpy().ravel()
    test_elapsed_time = test_df['elapsed_time'].to_numpy().ravel()
    print("The size of train dataset is ", len(X_train))
    print("The size of test dataset is ", len(X_test))
    X_cv_train, X_cv_test, y_cv_train, y_cv_test = train_test_split(X_train, y_train, test_size=0.25, random_state=42)
    min_data_in_leaf_calc = np.maximum(np.minimum(np.int32(len(X_cv_train)/1000),100),20)
    #declare gbm
    gbm = LGBMRegressor(metrics = ["mse", 'mae'],
            early_stopping_round = 100,
            min_child_samples = min_data_in_leaf_calc, learning_rate = 0.01, n_estimators= 1000, num_leaves = 63)
    ## fit function
    gbm.fit(
      X_cv_train,
      y_cv_train,
    eval_set=[(X_cv_test, y_cv_test)],
    verbose = False)
    onnx_pta_filename = 'pta_'+orig+dest+'.onnx'
    pickle_pta_filename = 'pta_'+orig+dest+'.pickle'
    onnx_path = os.getcwd() + '/od_onnx_models/' + onnx_pta_filename
    pickle_path = os.getcwd() + '/od_pickle_models/' + pickle_pta_filename
    pipeline_save = PipelineSaver()
    print("Fitting ML pta model - done")
    y_train_pred = gbm.predict(X_train)
    y_test_pred = gbm.predict(X_test)
    print("The accuracy of fitted model for the OD", orig+'-'+dest)
    print("The accuracy on train dataset is:")
    print(eval_results(y_train + train_elapsed_time, y_train_pred + train_elapsed_time))
    print("The accuracy on test dataset is:")
    print(eval_results(y_test + test_elapsed_time, y_test_pred + test_elapsed_time))
    print("Saving the PTA pipeline")
    pickle.dump(gbm, open(pickle_path,'wb'))
    print("Fitting PTA model - done")


sys.stdout = open('save_onnx_model.log', 'w')
features = ['IMO','Latitude', 'Longitude','elapsed_time','week','remaining_lead_time']
target = ['remaining_lead_time']
od_df = pd.read_csv("shipment_data.csv")
success = pd.read_csv("ods_sucessfully_processed.csv")
success['actualDeparturePort'] = success['OD'].str[0:5]
success['actualArrivalPort'] = success['OD'].str[6:11]
od_df['OD'] = od_df.actualDeparturePort + '-' +  od_df.actualArrivalPort
#ml_od_df = od_df[od_df.OD.isin(success.OD)].groupby(['actualDeparturePort','actualArrivalPort']).size().reset_index().rename(columns={0:'count'})
#ml_od_df = success.groupby(['actualDeparturePort','actualArrivalPort']).size().reset_index().rename(columns={0:'count'})
for orig, dest in zip(success.actualDeparturePort, success.actualArrivalPort):
    a = time.time()
    print('The OD being processed is', orig+'-'+dest)
    make_onnx_models(orig,dest)
    print("The time taken for created ONNX model for this OD in minutes is", np.round((time.time() - a)/60,2))

sys.stdout.close()