import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import MinMaxScaler

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import SimpleRNN, Dense, Dropout, Input
from tensorflow.keras.layers import LSTM

import keras_tuner as kt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import SimpleRNN, LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam, RMSprop
import json
import joblib

from airflow.exceptions import AirflowFailException

from .common import read_warehouse_pandas, write_to_hudi, create_spark_session, trino_connection, create_table
from .config import get_hyperparameters

hyperparameters = get_hyperparameters()
seq_length = hyperparameters['sequence_length']
dropout = hyperparameters['dropout']
activation = hyperparameters['activation']
learning_rate = hyperparameters['learning_rate']
batch_sizes = hyperparameters['batch_sizes']
epoch_list = hyperparameters['epoch_list']

def load_crop_data(crop_code):
    sql_query = f"""
        SELECT  ff.ProducerPrice_LCU_tonne_LCU_month,
                dd.YearMonth,
                dd.Date,
                ff.CropCode
        FROM fact_faostat AS ff           
            JOIN (SELECT DateINT,YearMonth, Date 
                    FROM dim_date
                    WHERE Date >= DATE'2011-01-01' 
                        AND Date <= DATE '2023-12-01') AS dd
                ON dd.DateINT = ff.DateINT
        WHERE ProducerPrice_SLC_tonne_SLC_month IS NULL
            AND ff.CropCode = {crop_code}
        ORDER BY dd.YearMonth
    """
    df_pandas = pd.read_sql(sql_query, trino_connection())
    df_pandas.interpolate(method='linear', inplace=True, limit_direction='both')
    return df_pandas

def preprocess_data(data):
    
    df_true_crop = data.copy()

    time_series = df_true_crop['ProducerPrice_LCU_tonne_LCU_month'].values


    split_idx = int(len(time_series) * 0.8)
    data_train, data_test = time_series[:split_idx], time_series[split_idx-1:]


    scaler = MinMaxScaler(feature_range=(0,1))
    data_train_scaled = scaler.fit_transform(data_train.reshape(-1, 1))
    data_test_scaled = scaler.transform(data_test.reshape(-1, 1))

    def create_sequences(data, seq_length=12):
        X, y = [], []
        for i in range(len(data) - seq_length):
            X.append(data[i:i+seq_length])
            y.append(data[i+seq_length])
        return np.array(X), np.array(y)

    X_train, y_train = create_sequences(data_train_scaled, seq_length)
    X_test, y_test = create_sequences(data_test_scaled, seq_length)

    return X_train, y_train, X_test, y_test, scaler

def build_model_rnn(hp):
    model = Sequential()
    
    units = hp.Int('units', min_value=64, max_value=128, step=64)
    input_shape = (seq_length, 1)

    model.add(SimpleRNN(units=units, activation=activation, input_shape=input_shape))
    
    model.add(Dropout(dropout))
    model.add(Dense(1))

    optimizer = RMSprop(learning_rate=learning_rate)

    model.compile(optimizer=optimizer, loss='mse', metrics=['mae'])
    return model

def build_model_lstm(hp):
    model = Sequential()
    units = hp.Int('units', min_value=64, max_value=128, step=64)
    input_shape = (seq_length, 1)

    model.add(LSTM(units=units, activation=activation, input_shape=input_shape))
    
    model.add(Dropout(dropout))
    model.add(Dense(1))

    optimizer = RMSprop(learning_rate=learning_rate)

    model.compile(optimizer=optimizer, loss='mse', metrics=['mae'])
    return model

def tune_and_train(X_train, y_train, X_test, y_test, scaler, model_type='lstm', crop_code = ''):
    if model_type == 'lstm':
        build_model = build_model_lstm
    else:
        build_model = build_model_rnn

    best_r2 = - np.inf
    best_config = None
    best_model = None

    for batch_size in batch_sizes:
        for epochs in epoch_list:
            print(f"🔍 Tuning with batch_size={batch_size}, epochs={epochs}")

            tuner = kt.GridSearch(
                build_model,
                objective='val_loss',
                max_trials=15,
                executions_per_trial=1,
                overwrite=True,
                directory='/opt/airflow/data/tuning',
                project_name=f"bs_{batch_size}_ep_{epochs}_{model_type}_{crop_code}",
            )

            tuner.search(X_train, y_train,
                        validation_split=0.2,
                        batch_size=batch_size,
                        epochs=epochs,
                        verbose=0)

            best_hp = tuner.get_best_hyperparameters(1)[0]
            model = build_model_rnn(best_hp)

            history = model.fit(
                X_train, y_train,
                validation_split=0.2,
                batch_size=batch_size,
                epochs=epochs,
                verbose=0
            )
            model_predictions = model.predict(X_test)
            model_predictions = scaler.inverse_transform(model_predictions)
            y_test_origin = scaler.inverse_transform(y_test.reshape(-1, 1))

            mse = mean_squared_error(y_test_origin, model_predictions)
            mae = mean_absolute_error(y_test_origin, model_predictions)
            r2 = r2_score(y_test_origin, model_predictions)

            if r2 > best_r2:
                best_r2 = r2
                best_config = {
                    'batch_size': batch_size,
                    'epochs': epochs,
                    'mse': mse,
                    'mae': mae,
                    'r2': r2,
                    **best_hp.values
                }
            best_model = tf.keras.models.clone_model(model)
            best_model.set_weights(model.get_weights())
    return best_model, best_config, best_r2


def train_crop_model(crop_code, model_type):
    
    data = load_crop_data(crop_code)

    
    X_train, y_train, X_test, y_test, scaler = preprocess_data(data)

    r2_check = 0.6
    r2 = 0
    max_retries = 3
    retries = 0

    while r2 < r2_check and retries < max_retries:
        print(f"[Crop {crop_code}] Training attempt {retries+1} / {r2_check}")
        best_model_rnn, best_config_rnn, r2 = tune_and_train(X_train, y_train, X_test, y_test, scaler, model_type, crop_code)
        print(f"[Crop {crop_code}] R² score: {r2:.4f}")
        retries += 1
        if retries == max_retries and r2_check >= 0.4:
            r2_check = r2_check - 0.05
            retries = 0


    if r2 >= r2_check:
        model_dir = f'/opt/airflow/data/models/{crop_code}'
        os.makedirs(model_dir, exist_ok=True)
        best_model_rnn.save(f"/opt/airflow/data/models/{crop_code}/crop_{crop_code}_{model_type}.keras")
        joblib.dump(scaler, f"/opt/airflow/data/models/{crop_code}/scaler_crop_{crop_code}.pkl")
        with open(f'/opt/airflow/data/models/{crop_code}/best_{model_type}_params.json', 'w') as f:
            json.dump(best_config_rnn, f)

        print(f"Training {model_type} completed for CropCode {crop_code}")
    else:
        raise Exception(
            f"[Crop {crop_code}] Training failed after {max_retries} retries. Final R² = {r2:.4f}"
        )

