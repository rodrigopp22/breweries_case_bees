import datetime
import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

SILVER_LAYER_PATH = "lake/2_silver"
TABLE_PATH = "lake/3_gold/tb_brewery_data"
FORMAT = "parquet"


def create_spark_session():
    conf = SparkConf().set("hive.exec.dynamic.partition", "true") \
        .set("hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark = SparkSession.builder.config(
        conf=conf).enableHiveSupport().getOrCreate()
    return spark


def read_data_from_silver(spark):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("brewery_type", StringType(), False),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])

    df = spark.read.schema(schema)\
        .format(FORMAT)\
        .load(SILVER_LAYER_PATH)\
        .select("country", "brewery_type")

    logging.info("Foram lidos %i registros da silver layer", df.count())

    return df


def create_aggregated_view(df):
    df_agg = df.groupBy("country", "brewery_type").count()
    logging.info("A view gerada contém %i registros", df_agg.count())
    return df_agg


def run():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Iniciando a execução do dia {datetime.date.today()}")
    spark = create_spark_session()
    df = read_data_from_silver(spark)
    df_agg = create_aggregated_view(df)
    logging.info("Salvando a view gerada...")
    df_agg.write\
        .mode("overwrite") \
        .format(FORMAT)\
        .parquet(TABLE_PATH)
    logging.info("Fim do processo.")

    spark.stop()


run()
