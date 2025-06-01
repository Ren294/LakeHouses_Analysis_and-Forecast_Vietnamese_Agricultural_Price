import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import numpy as np

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
from datetime import datetime
import joblib

from .common import read_warehouse_pandas, write_to_hudi, create_spark_session, trino_connection, create_table
from .config import get_all_crop_codes


def load_crop_data(crop_code):
    sql_query = f"""
        SELECT  ff.ProducerPrice_LCU_tonne_LCU_month,
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
    rnn_model = tf.keras.models.load_model(f'/opt/airflow/data/models/{crop_code}/crop_{crop_code}_rnn.keras')
    lstm_model = tf.keras.models.load_model(f'/opt/airflow/data/models/{crop_code}/crop_{crop_code}_lstm.keras')
    scaler = joblib.load(f'/opt/airflow/data/models/{crop_code}/scaler_crop_{crop_code}.pkl')
    return rnn_model, lstm_model, scaler


def predict_next(crop_code, seq_length = 6, prediction_length = 24):
    rnn_model, lstm_model, scaler = load_models(crop_code)

    df = load_crop_data(crop_code)

    scaled_prices = scaler.transform(df[['ProducerPrice_LCU_tonne_LCU_month']].values)
    last_seq = scaled_prices[-seq_length:].reshape(1, seq_length, 1)

    future_preds_rnn = []
    future_preds_lstm = []
    future_dates = []

    last_dateint = df['dateint'].values[-1]

    for i in range(prediction_length):
        pred_rnn = rnn_model.predict(last_seq, verbose=0)[0][0]
        pred_lstm = lstm_model.predict(last_seq, verbose=0)[0][0]

        future_preds_rnn.append(pred_rnn)
        future_preds_lstm.append(pred_lstm)

        last_year = int(str(last_dateint)[:4])
        last_month = int(str(last_dateint)[4:6])
        new_month = last_month + 1
        new_year = last_year + new_month // 13
        new_month = new_month % 13 if new_month % 13 != 0 else 1

        next_dateint = int(f"{new_year}{new_month:02d}")
        future_dates.append(next_dateint)
        last_dateint = next_dateint
        last_seq = np.append(last_seq[:, 1:, :], [[[pred_lstm]]], axis=1)

    future_preds_rnn = scaler.inverse_transform(np.array(future_preds_rnn).reshape(-1, 1)).reshape(-1)
    future_preds_lstm = scaler.inverse_transform(np.array(future_preds_lstm).reshape(-1, 1)).reshape(-1)
    result = pd.DataFrame({
        'DateINT': [yearmonth*100+1 for yearmonth in future_dates],
        'CropCode': crop_code,
        'PredictRNN': future_preds_rnn,
        'PredictLSTM': future_preds_lstm
    })
    result['timestamp'] = datetime.now()
    result['recordId'] = result['CropCode'].astype(str) + "_" + result['DateINT'].astype(str)
    return result

def predict_crop_price(crop_code = None, path = ''):
    if crop_code is None:
        crop_codes = get_all_crop_codes()
        predict_data = pd.DataFrame()
        for code in crop_codes:
            predict_1crops = predict_next(code, prediction_length=60)
            predict_data = pd.concat([predict_data, predict_1crops], ignore_index=True)
    else:
        predict_data = predict_next(crop_code,prediction_length=60)
    print("Started spark session")
    spark = create_spark_session("PricePrediction")
    df_spark = spark.createDataFrame(predict_data)
    print("Writing to Hudi")
    write_to_hudi(df_spark, "predict_crop_price", path, recordkey="recordId",
                    precombine="timestamp", mode="append")
    print("Creating table")
    create_table(spark, "predict_crop_price", path)
    print(f"Predict completed for CropCode {crop_code}")

