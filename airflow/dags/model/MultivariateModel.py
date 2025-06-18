import pandas as pd
import numpy as np


from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import SimpleRNN, Dense, Dropout, Input
from tensorflow.keras.layers import LSTM
from tensorflow.keras.optimizers import Adam, RMSprop
from tensorflow.keras.preprocessing.sequence import TimeseriesGenerator
import keras_tuner as kt

import os
import json
import joblib
import tempfile

from .common import trino_connection, minio_client, detrend, deseason, scale_minmax
from .config import get_hyperparameters

hyperparameters = get_hyperparameters()
num_lstm_layer_list = hyperparameters['num_lstm_layer']
num_rnn_layer_list = hyperparameters['num_rnn_layer']
sequence_lengths = hyperparameters['sequence_lengths']
dropout = hyperparameters['dropout']
activation = hyperparameters['activation']
learning_rate = hyperparameters['learning_rate']
epoch_list = hyperparameters['epoch_list']
units_list = hyperparameters['units']
features = ["Price", "Temp", "Humidity", "Precip", "Solarenergy"]

def load_crop_data(crop_code):
    sql_query = f"""
        SELECT  ff.ProducerPrice_LCU_tonne_LCU_month as Price,
        		fw.Temp, fw.Humidity, fw.Precip, fw.Solarenergy,
                dd.YearMonth,
                dd.Date,
                ff.CropCode
        FROM "default".fact_faostat AS ff           
            JOIN (SELECT DateINT,YearMonth, Date 
                    FROM "default".dim_date
                    WHERE Date >= DATE'2011-01-01' 
                        AND Date <= DATE '2023-12-01') AS dd
                ON dd.DateINT = ff.DateINT
            JOIN (
            		Select dateint, Avg(temp) as Temp, Avg(humidity) as Humidity, 
            			Avg(precip) as Precip, Avg(solarenergy) as Solarenergy
					FROM"default".fact_weather 
					group by dateint
				)AS fw
                ON fw.DateINT = ff.DateINT
        WHERE ProducerPrice_SLC_tonne_SLC_month IS NULL
            AND ff.CropCode = {crop_code}
        ORDER BY dd.YearMonth
    """
    df_pandas = pd.read_sql(sql_query, trino_connection())

    df_pandas.interpolate(method='linear', inplace=True, limit_direction='both')

    time_series = df_pandas[features].values

    return time_series

def preprocess_data(time_series):
    time_series_price = time_series[:, 0]  # Extract the Price column 

    time_series_detrended, trend_model = detrend(time_series_price)
    time_series_deseasoned, seasonal_avg =  deseason(time_series_detrended)

    time_series[:, 0] = time_series_deseasoned  # Replace the Price column with deseasoned data
    time_series_scaled, scaler = scale_minmax(time_series)
    
    return time_series_scaled, trend_model, seasonal_avg, scaler

def build_model_rnn(hp, input_shape):

    model = Sequential()
    num_rnn_layer = hp.Choice(f'num_rnn_layer', num_rnn_layer_list)
    units = hp.Choice(f'units', units_list)
    dropout = 0.1

    model.add(Input(shape=input_shape))
    for i in range(num_rnn_layer):
        return_sequences = (i < num_rnn_layer - 1)

        model.add(SimpleRNN(units, activation='tanh',
                           return_sequences=return_sequences))
        if dropout > 0:
            model.add(Dropout(dropout))

    model.add(Dense(1))
    model.compile(optimizer=RMSprop(0.001), loss='mse')

def build_model_lstm(hp, input_shape):

    model = Sequential()
    num_lstm_layer = hp.Choice(f'num_lstm_layer', num_lstm_layer_list)
    units = hp.Choice(f'units', units_list)
    dropout = 0.1

    model.add(Input(shape=input_shape))
    for i in range(num_lstm_layer):
        return_sequences = (i < num_lstm_layer - 1)
        
        model.add(LSTM(units, activation='tanh',
                           return_sequences=return_sequences))
        if dropout > 0:
            model.add(Dropout(dropout))

    model.add(Dense(1))
    model.compile(optimizer=RMSprop(0.001), loss='mse')
    
    return model

def tune_and_train(time_series, model_type='lstm', crop_code = ''):
    best_result = None
    best_loss = float('inf')
    hp = kt.HyperParameters()
    epochs = hp.Choice('epochs', [100, 200])

    for seq_len in sequence_lengths:
        print(f"Tuning with seq_len = {seq_len}")

        split_index = int(len(time_series) * 0.8)
        y_train = time_series[:split_index]
        y_val = time_series[split_index:]

        train_gen = TimeseriesGenerator(y_train, y_train[:, 0], length=seq_len, batch_size=16)
        val_gen = TimeseriesGenerator(y_val, y_val[:, 0], length=seq_len, batch_size=16)

        input_shape = (seq_len, len(features))

        tuner = kt.GridSearch(
            hypermodel=lambda hp: build_model_lstm(hp, input_shape),
            objective='val_loss',
            max_trials=20,
            executions_per_trial=1,
            directory='lstm_tuning',
            project_name=f'seq_len_{seq_len}',
            overwrite=True 
        )
        
        tuner.search(train_gen, validation_data=val_gen, epochs=epochs, verbose = 0)

        best_hp = tuner.get_best_hyperparameters(1)[0]
        best_model = tuner.get_best_models(1)[0]
        val_loss = best_model.evaluate(val_gen, verbose=0)

        print(f"Best loss for seq_len={seq_len}: {val_loss}")

        if val_loss < best_loss:
            best_loss = val_loss
            best_result = best_hp.values
            best_result['seq_len'] = seq_len
            best_result['val_loss'] = val_loss
            

    return best_model, best_result


def save_model(crop_code, model, hp, trend_model, seasonal_avg, scaler, model_type = 'lstm'):
    bucket_name = "models"
    client = minio_client(bucket_name)

    prefix = f'/v3/{crop_code}/'

    # 1. Save model
    model_path = os.path.join(tempfile.gettempdir(), f"model_{model_type}.keras")
    model.save(model_path)
    client.fput_object(bucket_name, prefix + f"model_{model_type}.keras", model_path)

    # 2. Save hyperparameters
    hp_path = os.path.join(tempfile.gettempdir(), "hyperparams.json")
    with open(hp_path, 'w') as f:
        json.dump(hp, f)
    client.fput_object(bucket_name, prefix + "hyperparams.json", hp_path)

    # 3. Save scaler
    scaler_path = os.path.join(tempfile.gettempdir(), "scaler.pkl")
    joblib.dump(scaler, scaler_path)
    client.fput_object(bucket_name, prefix + "scaler.pkl", scaler_path)

    # 4. Save trend_fit
    trend_path = os.path.join(tempfile.gettempdir(), "trend_model.pkl")
    joblib.dump(trend_model, trend_path)
    client.fput_object(bucket_name, prefix + "trend_model.pkl", trend_path)

    # 5. Save seasonal_avg
    seasonal_path = os.path.join(tempfile.gettempdir(), "seasonal_avg.pkl")
    joblib.dump(seasonal_avg, seasonal_path)
    client.fput_object(bucket_name, prefix + "seasonal_avg.pkl", seasonal_path)
    

def train_crop_model(crop_code, model_type):
    
    time_series = load_crop_data(crop_code)
    
    time_series, trend_model, seasonal_avg, scaler= preprocess_data(time_series)

    print(f"INFO - time_series.shape: {time_series.shape}")
    
    best_model, best_result = tune_and_train(time_series)
    save_model(crop_code, best_model, best_result, trend_model, seasonal_avg, scaler, model_type)


        

