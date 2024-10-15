import logging
from datetime import datetime, timedelta


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

BRONZE_LAYER_PATH = "lake/1_bronze"
FORMAT = 'parquet'
SILVER_LAYER_PATH = "lake/2_silver/tb_brewery_data"

def run():
    conf = SparkConf().set("hive.exec.dynamic.partition", "true") \
        .set("hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark = SparkSession.builder.config(
        conf=conf).enableHiveSupport().getOrCreate()

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
    
    logging.info("Lendo dados em %s", BRONZE_LAYER_PATH)
    
    df = spark.read.schema(schema)\
                   .option('multiline', 'true')\
                   .json(f'{BRONZE_LAYER_PATH}/*.json')


    logging.info("Salvando %i registros na tb_brewery_data", df.count())
    
    df.write.mode('overwrite') \
            .format(FORMAT)\
            .partitionBy('country') \
            .parquet(SILVER_LAYER_PATH)
            
run()