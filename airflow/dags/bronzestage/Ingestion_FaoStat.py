from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from common import create_spark_session, write_to_hudi
from config import get_parameter_sets_faostat


def load_data_to_hudi(domain_code, record_id_cols, table_name, path):
    spark = create_spark_session(f"FaoStat {table_name} to Hudi on MinIO")
    vietnam_code = 237
    mypars = {
        'area': vietnam_code
    }

    data = faostat.get_data_df(domain_code, pars=mypars, strval=False)
    df = spark.createDataFrame(data)
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", ""))
    df = df.withColumn("record_id", F.concat_ws("_", *[F.col(col) for col in record_id_cols])) \
           .withColumn("timestamp", F.to_timestamp(F.col("Year").cast("string"), "yyyy"))
    df = df.na.drop(subset=["record_id"])
    df = df.withColumn("Year", F.col("Year").cast("int"))
    df = df.withColumn("Value", F.col("Value").cast("float"))
    write_to_hudi(df, table_name, path, partitionpath="ElementCode")
    spark.stop()


def FaoStatBronze(path):
    parameter_sets = get_parameter_sets_faostat()
    for params in parameter_sets:
        load_data_to_hudi(
            domain_code=params["domain_code"],
            record_id_cols=params["record_id_cols"],
            table_name=params["table_name"],
            path=path
        )


# if __name__ == "__main__":
#     path = "s3a://bronze/faostat_data"
#     main(path)
