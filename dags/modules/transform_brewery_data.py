import logging
import datetime


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

BRONZE_LAYER_PATH = "/opt/airflow/data/1_bronze"
FORMAT = 'parquet'
SILVER_LAYER_PATH = "/opt/airflow/data/2_silver/tb_brewery_data"

date_now = datetime.date.today()


def create_spark_session():
    conf = SparkConf().set("hive.exec.dynamic.partition", "true") \
        .set("hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark = SparkSession.builder.config(
        conf=conf).enableHiveSupport().getOrCreate()
    return spark


def read_data_from_bronze(spark, file_path: str) -> DataFrame:
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
                   .json(file_path)
    return df


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Iniciando a execução do dia {date_now}")
    spark = create_spark_session()

    bronze_path = f'{BRONZE_LAYER_PATH}/*.json'
    df_bronze = read_data_from_bronze(spark, bronze_path)

    logging.info(
        "Foram lidos %i registros da camada bronze",
        df_bronze.count())

    # removendo registros que podem ter país com valor nulo ou tipo de brewery
    # nulo, removendo ids duplicados
    df_clean = df_bronze.filter((df_bronze.country.isNotNull()) & (df_bronze.brewery_type.isNotNull()))\
                        .drop_duplicates(["id"])\
                        .withColumn("country", trim(col("country")))  # existe um caso para United States que está escrito com um espaço a mais

    logging.info("Dados limpos, %i registros", df_clean.count())

    logging.info("Escrevendo %i registros na silver layer", df_clean.count())

    # repartition para melhorar a escrita dos arquivos por conta de small files
    df_clean.repartition("country")\
            .write.mode('overwrite') \
            .format(FORMAT)\
            .partitionBy('country') \
            .parquet(SILVER_LAYER_PATH)

    logging.info("Fim do processo.")

    spark.stop()


if __name__ == '__main__':
    run()
