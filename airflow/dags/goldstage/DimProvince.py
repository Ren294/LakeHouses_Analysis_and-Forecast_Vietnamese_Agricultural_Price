from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import write_to_hudi, create_spark_session, create_table
from .config import get_province
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup


def create_dim_province(spark, path):
    #url = "https://en.wikipedia.org/wiki/Provinces_of_Vietnam"
    url = "https://en.wikipedia.org/w/index.php?title=Provinces_of_Vietnam&diff=prev&oldid=1286631665"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table', {'class': 'wikitable sortable'})

    headers = [th.get_text(strip=True) for th in table.find_all('th')]

    data = []
    for row in table.find_all('tr')[1:]:  # Skip header
        cols = row.find_all(['td', 'th'])
        cols = [col.get_text(strip=True) for col in cols]
        if cols:
            data.append(cols)
    df = pd.DataFrame(data, columns=headers)
    df.columns = ['ProvinceNameUtf8', 'ProvinceCode', 'Center', 'Area',
                  'Population', 'Density', 'Urban', 'HDI', 'GDP', 'Region']
    provinces = get_province()
    df["Region"] = df["Region"].replace("", pd.NA).fillna(method="ffill")
    df["ProvinceNameUtf8"] = df["ProvinceNameUtf8"].str.replace(r"\s*province$", "", regex=True, case=False)\
        .replace("Hanoi", "Hà Nội")\
        .replace("Haiphong", "Hải Phòng")\
        .replace("Ho Chi Minh City", "Hồ Chí Minh")\
        .replace("Huế", "Thừa Thiên Huế")\
        .replace("Đăk Nông", "Đắk Nông")\
        .replace("Bà Rịa–Vũng Tàu", "Bà Rịa - Vũng Tàu")\
        .replace("Da Nang", "Đà Nẵng")
    df["ProvinceName"] = [provinces[name] for name in df['ProvinceNameUtf8']]
    df['Timestamp'] = datetime.now()
    df['Area'] = df['Area'].replace({',': ''}, regex=True).astype(float)
    df['ProvinceCode'] = df['ProvinceCode'].astype(int)
    df = df.drop(['Density', 'Urban', 'HDI', 'GDP',
                 'Population', 'Center'], axis=1)
    spark_df = spark.createDataFrame(df)
    write_to_hudi(spark_df, "dim_province", path, recordkey="ProvinceCode", precombine="Timestamp")
    create_table(spark, "dim_province", path)


def DimProvinceGold(path):
    spark = create_spark_session("DimProvince")
    create_dim_province(spark, path)
    spark.stop()
