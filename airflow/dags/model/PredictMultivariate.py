import pandas as pd
import numpy as np
import json
import joblib
import tempfile
from datetime import datetime

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import MinMaxScaler

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import SimpleRNN, Dense, Dropout, Input
from tensorflow.keras.layers import LSTM
import keras_tuner as kt

from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import SimpleRNN, LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam, RMSprop


from .common import write_to_hudi, create_spark_session, trino_connection, create_table, minio_client,\
     detrend, deseason, scale_minmax, inverse_transform, add_season, add_trend, scale_custom_range
from .config import get_all_crop_codes
features = ['Price', 'Temp', 'Humidity', 'Precip', 'Solarenergy']

def load_crop_data(crop_code):
    sql_query = f"""
        SELECT  ff.ProducerPrice_LCU_tonne_LCU_month as Price,
        		fw.Temp, fw.Humidity, fw.Precip, fw.Solarenergy,
                dd.dateint,
                dd.Date,
                ff.CropCode
        FROM "default".fact_faostat AS ff           
            JOIN (SELECT DateINT,YearMonth, Date 
                    FROM "default".dim_date
                    WHERE Date >= DATE'2011-01-01' 
                        AND Date <= DATE '2028-12-01') AS dd
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
    return df_pandas

def load_models(crop_code):
    bucket_name = "models"
    prefix = f"v3/{crop_code}/" 
    client = minio_client(bucket_name)
    tmpdir = tempfile.mkdtemp()

    def load_from_minio(filename):
        local_path = f"{tmpdir}/{filename}"
        client.fget_object(bucket_name, prefix + filename, local_path)
        return local_path

    model_path = load_from_minio("model_lstm.keras")  # hoặc model.h5
    model = load_model(model_path)
    with open(load_from_minio("hyperparams.json"), "r") as f:
        hyperparams = json.load(f)
    scaler = joblib.load(load_from_minio("scaler.pkl"))
    trend_model = joblib.load(load_from_minio("trend_model.pkl"))
    seasonal_avg = joblib.load(load_from_minio("seasonal_avg.pkl"))

    return model, hyperparams, trend_model, seasonal_avg, scaler

def preprocess_data(data, trend_model, seasonal_avg, scaler):
    price = data[:, 0]
    price_detrended, _ = detrend(price, trend_model)
    price_deseasoned, _ = deseason(price_detrended, seasonal_avg)
    data[:, 0] = price_deseasoned 
    data_scaled, _ = scale_minmax(data, scaler)
    return data_scaled


def repreprocess_data(time_series, trend_model, seasonal_avg, scaler, pedict_indx, prediction_length):

    n_preds = time_series.shape[0]
    dummy_input = np.zeros((n_preds, 5)) 
    dummy_input[:, 0] = time_series.flatten()
    full_inversed  = inverse_transform(dummy_input, scaler)
    time_series_inversed = full_inversed[:, 0].reshape(-1)
    
    #time_series_inversed = inverse_transform(time_series, scaler)
    time_series_addseasoned =  add_season(time_series_inversed, seasonal_avg)
    time_series_addtrended = add_trend(time_series_addseasoned, trend_model, pedict_indx, prediction_length)
    return time_series_addtrended

def create_sequences(data, seq_length):
    X = []
    for i in range(len(data) - seq_length):
        X.append(data[i:i+seq_length])
    return np.array(X)

def predict_next(crop_code, seq_length = 12, prediction_length = 60):
    model, hyperparams, trend_model, seasonal_avg, scaler = load_models(crop_code)

    seq_length = hyperparams['seq_len']

    df = load_crop_data(crop_code)
    
    data = df[features].values
    scaled_data = preprocess_data(data, trend_model, seasonal_avg, scaler)

    pedict_indx = len(scaled_data)

    last_seq = scaled_data[-seq_length:].reshape(1, seq_length, -1)

    future_preds = []
    future_dates = []

    last_dateint = df['dateint'].values[-1]

    for i in range(prediction_length):
        pred_lstm = model.predict(last_seq, verbose=0)[0][0]
    
        future_preds.append(pred_lstm)

        last_weather = last_seq[0, -1, 1:] 
        new_input = np.append(pred_lstm, last_weather).reshape(1, 1, -1)

        last_year = int(str(last_dateint)[:4])
        last_month = int(str(last_dateint)[4:6])
        new_month = last_month + 1
        new_year = last_year + new_month // 13
        new_month = new_month % 13 if new_month % 13 != 0 else 1

        next_dateint = int(f"{new_year}{new_month:02d}")
        future_dates.append(next_dateint)
        last_dateint = next_dateint

        last_seq = np.append(last_seq[:, 1:, :], new_input, axis=1)

    future_preds = np.array(future_preds).reshape(-1, 1)
    final_preds = repreprocess_data(future_preds, trend_model, seasonal_avg, scaler,
                                 pedict_indx, prediction_length).reshape(-1)
    result = pd.DataFrame({
        'DateINT': [yearmonth*100+1 for yearmonth in future_dates],
        'CropCode': crop_code,
        'PredictLSTM': final_preds
    })
    result['timestamp'] = datetime.now()
    result['recordId'] = result['CropCode'].astype(str) + "_" + result['DateINT'].astype(str)
    return result

def predict_crop_price(crop_code = None, path = ''):
    path = "s3a://predict//predict_crop_price_weather"
    table_name = "predict_crop_price_weather"
    if crop_code is None:
        crop_codes = get_all_crop_codes()
        predict_data = pd.DataFrame()
        for code in crop_codes:
            predict_1crops = predict_next(code, seq_length=12, prediction_length=60)
            predict_data = pd.concat([predict_data, predict_1crops], ignore_index=True)
    else:
        predict_data = predict_next(crop_code, seq_length=12, prediction_length=60)
    print("Started spark session")
    spark = create_spark_session("PricePrediction")
    df_spark = spark.createDataFrame(predict_data)
    print("Writing to Hudi")
    write_to_hudi(df_spark, table_name, path, recordkey="recordId",
                    precombine="timestamp", mode="append")
    print("Creating table")
    create_table(spark, table_name, path)
    print(f"Predict completed for CropCode {crop_code}")

