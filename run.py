__author__ = 'ag300g'

import pandas as pd
import numpy as np
import os
import sys
import yaml
import lib.models as models

def main(setting_file):
    params = yaml.load(open(setting_file, 'r'))
    df = read_data(params)
    preprocessing(df, params)
    res_df_train, res_df_test = forecast(df, params)
    postprocessing(res_df_train, res_df_test, params)


def get_feature_columns(all_columns, params):
    if 'feature_columns' in params \
            and len(params['feature_columns']) > 0:
        return params['feature_columns']
    rm_cols = []
    rm_cols.extend(params['keys_time'])
    rm_cols.extend(params['keys_batch'])
    rm_cols.extend([params['target_column']])
    if 'feature_rm_columns' in params \
            and len(params['feature_rm_columns']) > 0:
        rm_cols.extend(params['feature_rm_columns'])
    return [x for x in all_columns if x not in rm_cols]



def preprocessing(df, params):
    df.fillna(method='ffill', inplace=True)
    df.fillna(method='bfill', inplace=True)


def postprocessing(df1, df2, params):
    df1[df1['y_'] < 0] = 0
    df2[df2['y_'] < 0] = 0
    print(mape_calc(df1, params))
    print(mape_calc(df2, params))


def mape_calc(df, params):
    a = df[params['target_column']]
    b = df['y_']
    mape = abs(a-b) / a
    mape.replace([np.inf, -np.inf, np.nan], inplace=True)
    return sum(mape <= 0.15)/len(mape), abs(sum(a)-sum(b))/sum(a)


def forecast(df, params):
    model = getattr(models, params['method'])
    feature_columns = get_feature_columns(df.columns, params)
    res_train = []
    res_test = []
    grouped = df.groupby(params['keys_batch'])
    for name, df_i in grouped:
        df_train = df_i[df_i[params['split_name']] <= params['train_max']]
        df_test = df_i[df_i[params['split_name']].isin(params['test_values'])]

        if len(df_train) > 0 and len(df_test) > 0:
            y_train = df_train[params['target_column']]
            X_train = df_train[feature_columns]
            y_test = df_test[params['target_column']]
            X_test = df_test[feature_columns]
            pred_train, pred_test = model(X_train, y_train, X_test)
            df_train['y_'] = pred_train
            df_test['y_'] = pred_test
            res_train.append(df_train)
            res_test.append(df_test)
    res_df_train = pd.concat(res_train, axis=0)
    res_df_test = pd.concat(res_test, axis=0)
    return res_df_train, res_df_test


def read_data(params):
    # read Y varibles
    df = pd.read_table(params['target_file'])

    # read X variables part 1 (time related features)
    if params['time_features_dir'] is not None:
        for dirpath, dirnames, filenames in os.walk(params['time_features_dir']):
            for filename in filenames:
                f = os.path.join(dirpath, filename)
                tmp = pd.read_table(f)
                df = pd.merge(df, tmp,
                              on=params['keys_time'],
                              how='left')

    # read X variables part 2 (category related features)
    if params['category_features_dir'] is not None:
        for dirpath, dirnames, filenames in os.walk(params['category_features_dir']):
            for filename in filenames:
                f = os.path.join(dirpath, filename)
                tmp = pd.read_table(f)
                df = pd.merge(df, tmp,
                              on=params['keys_group']+params['keys_time'],
                              how='left')
    return df


if __name__ == '__main__':
    # read command line arguments
    n = len(sys.argv) - 1
    if n != 1:
        print('Usage: \n    python run.py setting_file\n')
        sys.exit()
    else:
        setting_file = sys.argv[1]
        print("[INFO] top down forecasting started ...")
        main(setting_file)
        print("[INFO] top down forecasting done!")
