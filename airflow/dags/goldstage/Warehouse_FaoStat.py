from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import read_from_hudi, write_to_hudi, create_spark_session, create_table


def create_fact_faostat(spark, path):
    try:
        faostat_df = read_from_hudi(
            spark, "s3a://silver/faostat_data", "faostat_merged")
        faostat_df = faostat_df.withColumn(
            "DateINT",
            date_format(col("timestamp"), "yyyyMM01").cast("int")
        ).withColumn(
            "recordid",
            monotonically_increasing_id()
        )
        fact_faostat = faostat_df.select(
            col("recordid"),
            col("DateINT"),
            col("ItemCode").alias("CropCode").cast("int"),
            col("Areaharvested_I_5312_I_ha_I_year").alias(
                "AreaHarvested_ha_year"),
            col("Production_I_5510_I_t_I_year").alias("Production_t_year"),
            col("Yield_I_5412_I_kg_ha_I_year").alias("Yield_kg_ha_year"),
            col("GrossProductionIndexNumber20142016100_I_432_I__I_year").alias(
                "GrossProductionIndexNumber_2014_2016_100_year"),
            col("GrosspercapitaProductionIndexNumber20142016100_I_434_I__I_year").alias(
                "GrosspercapitaProductionIndexNumber_2014_2016_100_year"),
            col("GrossProductionValueconstant20142016thousandI_I_152_I_1000_Int__I_year").alias(
                "GrossProductionValueconstant_2014_2016_thousand_1000_Int_year"),
            col("GrossProductionValueconstant20142016thousandSLC_I_55_I_1000_SLC_I_year").alias(
                "GrossProductionValueconstant_2014_2016_thousandSLC_1000_SLC_year"),
            col("GrossProductionValueconstant20142016thousandUS_I_58_I_1000_USD_I_year").alias(
                "GrossProductionValueconstant_2014_2016_thousandUS_1000_USD_year"),
            col("GrossProductionValuecurrentthousandSLC_I_56_I_1000_SLC_I_year").alias(
                "GrossProductionValuecurrent_thousandSLC_1000_SLC_year"),
            col("GrossProductionValuecurrentthousandUS_I_57_I_1000_USD_I_year").alias(
                "GrossProductionValuecurrent_thousandUS_1000_USD_year"),
            col("ExportQuantity_I_5910_I_t_I_year").alias(
                "ExportQuantity_t_year"),
            col("ExportValue_I_5922_I_1000_USD_I_year").alias(
                "ExportValue_1000_USD_year"),
            col("ImportQuantity_I_5610_I_t_I_year").alias(
                "ImportQuantity_t_year"),
            col("ImportValue_I_5622_I_1000_USD_I_year").alias(
                "ImportValue_1000_USD_year"),
            col("ProducerPriceIndex20142016100_I_5539_I__I_month").alias(
                "ProducerPriceIndex_2014_2016_100_month"),
            col("ProducerPriceLCUtonne_I_5530_I_LCU_I_month").alias(
                "ProducerPrice_LCU_tonne_LCU_month"),
            col("ProducerPriceSLCtonne_I_5531_I_SLC_I_month").alias(
                "ProducerPrice_SLC_tonne_SLC_month"),
            col("ProducerPriceUSDtonne_I_5532_I_USD_I_month").alias(
                "ProducerPrice_USD_tonne_USD_month")
        )

        write_to_hudi(fact_faostat, "fact_faostat", path,
                      recordkey="recordid", precombine="DateINT")

        create_table(spark, "fact_faostat", path)

    except Exception as e:
        print(f"Error in creating FAO STAT fact table: {str(e)}")
        raise


def FaoStatGold(path: str) -> None:
    spark = create_spark_session("Warehouse_FaoStat")
    create_fact_faostat(spark, path)
    spark.stop()


# if __name__ == "__main__":
#     spark = create_spark_session("Warehouse_FaoStat")
#     path = "s3a://gold/warehouse/fact_faostat"
#     create_fact_faostat(spark, path)
#     spark.stop()
