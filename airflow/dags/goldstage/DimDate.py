from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from .common import write_to_hudi, create_spark_session, create_table
import pandas as pd
from datetime import datetime
import json


def generate_dim_date(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    dim_date = pd.DataFrame({
        "DateINT": date_range.strftime('%Y%m%d').astype(int),
        "Date": date_range,
        "Day": date_range.day,
        "DayOfWeek": date_range.day_name(),
        "DayOfWeekNumber": date_range.weekday + 1,
        "DayOfWeekShort": date_range.strftime('%a'),
        "MonthNameLong": date_range.strftime('%B'),
        "MonthNameShort": date_range.strftime('%b'),
        "Month": date_range.month,
        "Quarter": (date_range.month - 1) // 3 + 1,
        "QuarterName": date_range.quarter.map({1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}),
        "WeekOfYear": date_range.isocalendar().week,
        "WeekDayFlag": ~date_range.weekday.isin([5, 6]),
        "IsWeekend": date_range.weekday.isin([5, 6]),
        "IsHoliday": False,
        "Year": date_range.year,
        "YearMonth": date_range.strftime('%Y%m').astype(int),
        "YearQuarter": date_range.year * 10 + ((date_range.month - 1) // 3 + 1),
        "IsLeapYear": date_range.year % 4 == 0,
        "Season": date_range.month.map({
            12: 'Winter', 1: 'Spring', 2: 'Spring',
            3: 'Spring', 4: 'Summer', 5: 'Summer',
            6: 'Summer', 7: 'Fall', 8: 'Fall',
            9: 'Fall', 10: 'Winter', 11: 'Winter'
        })
    })
    return dim_date


def create_dim_date(spark, path):
    schema = StructType([
        StructField("DateINT", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("Day", IntegerType(), True),
        StructField("DayOfWeek", StringType(), True),
        StructField("DayOfWeekNumber", IntegerType(), True),
        StructField("DayOfWeekShort", StringType(), True),
        StructField("MonthNameLong", StringType(), True),
        StructField("MonthNameShort", StringType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Quarter", IntegerType(), True),
        StructField("QuarterName", StringType(), True),
        StructField("WeekOfYear", IntegerType(), True),
        StructField("WeekDayFlag", BooleanType(), True),
        StructField("IsWeekend", BooleanType(), True),
        StructField("IsHoliday", BooleanType(), True),
        StructField("Year", IntegerType(), True),
        StructField("YearMonth", IntegerType(), True),
        StructField("YearQuarter", IntegerType(), True),
        StructField("IsLeapYear", BooleanType(), True),
        StructField("Season", StringType(), True),
    ])

    dim_date_df = generate_dim_date("1970-01-01", "2030-01-01")
    spark_df = spark.createDataFrame(dim_date_df, schema=schema)
    write_to_hudi(spark_df, "dim_date", path,
                  recordkey="DateINT", precombine="DateINT")
    create_table(spark, "dim_date", path)


def DimDateGold(path):
    spark = create_spark_session("DimDate")
    create_dim_date(spark, path)
    spark.stop()
