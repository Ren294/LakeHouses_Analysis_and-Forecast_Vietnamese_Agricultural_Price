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
     detrend, deseason, scale_minmax, inverse_transform, add_season, add_trend
from .config import get_all_crop_codes


def load_crop_data(crop_code):
    sql_query = f"""
        SELECT  ff.ProducerPrice_LCU_tonne_LCU_month as Price,
                dd.dateint,
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

def load_models(crop_code):
    bucket_name = "models"
    prefix = f"v1/{crop_code}/" 
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

def preprocess_data(time_series, trend_model, seasonal_avg, scaler):
    time_series_detrended, _ = detrend(time_series, trend_model)
    time_series_deseasoned, _ =  deseason(time_series_detrended, seasonal_avg)
    time_series_scaled, _  = scale_minmax(time_series_deseasoned, scaler)
    return time_series_scaled

def repreprocess_data(time_series, trend_model, seasonal_avg, scaler, pedict_indx, prediction_length):
    time_series_inversed = inverse_transform(time_series, scaler)
    time_series_addseasoned =  add_season(time_series_inversed, seasonal_avg)
    time_series_addtrended = add_trend(time_series_addseasoned, trend_model, pedict_indx, prediction_length)
    return time_series_addtrended


def predict_next(crop_code, seq_length = 12, prediction_length = 60):
    model, hyperparams, trend_model, seasonal_avg, scaler = load_models(crop_code)

    df = load_crop_data(crop_code)
    
    scaled_prices = preprocess_data(df[['Price']].values.flatten(), trend_model, seasonal_avg, scaler)
    pedict_indx = len(scaled_prices)
    last_seq = scaled_prices[-seq_length:].reshape(1, seq_length, 1)

    future_preds = []
    future_dates = []

    last_dateint = df['dateint'].values[-1]

    for i in range(prediction_length):
        pred_lstm = model.predict(last_seq, verbose=0)[0][0]
    
        future_preds.append(pred_lstm)

        last_year = int(str(last_dateint)[:4])
        last_month = int(str(last_dateint)[4:6])
        new_month = last_month + 1
        new_year = last_year + new_month // 13
        new_month = new_month % 13 if new_month % 13 != 0 else 1

        next_dateint = int(f"{new_year}{new_month:02d}")
        future_dates.append(next_dateint)
        last_dateint = next_dateint
        last_seq = np.append(last_seq[:, 1:, :], [[[pred_lstm]]], axis=1)

    future_preds = repreprocess_data(np.array(future_preds).reshape(-1, 1),
                                      trend_model, seasonal_avg, scaler, pedict_indx, prediction_length).reshape(-1)
    result = pd.DataFrame({
        'DateINT': [yearmonth*100+1 for yearmonth in future_dates],
        'CropCode': crop_code,
        'PredictLSTM': future_preds
    })
    result['timestamp'] = datetime.now()
    result['recordId'] = result['CropCode'].astype(str) + "_" + result['DateINT'].astype(str)
    return result

def predict_crop_price(crop_code = None, path = ''):
    path = 's3a://predict//predict_crop_price2'
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
    write_to_hudi(df_spark, "predict_crop_price2", path, recordkey="recordId",
                    precombine="timestamp", mode="append")
    print("Creating table")
    create_table(spark, "predict_crop_price2", path)
    print(f"Predict completed for CropCode {crop_code}")

