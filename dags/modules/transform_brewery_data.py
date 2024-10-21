import logging
import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from dags.modules.utils.EnvironmentVariables import EnvironmentVariables


def create_spark_session() -> SparkSession:
    '''
    Cria e configura uma sessão Spark.
    '''
    conf = (SparkConf()
            .setAppName("load_tb_brewery_by_location")
            .setMaster("local")
            .set("spark.executors.instances", 3)
            .set("spark.executors.cores", 5)
            .set("spark.executor.memory", "2g"))
    spark = SparkSession.builder.config(
        conf=conf).enableHiveSupport().getOrCreate()
    return spark


def get_schema() -> StructType:
    '''
    Retorna o schema pré-definido de uma tabela.
    '''
    return StructType([
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


def read_json_from_path(spark: SparkSession, schema: StructType, file_path: str) -> DataFrame:
    """
    Lê dados de um arquivo JSON a partir de um caminho especificado, utilizando um schema predefinido.

    Argumentos:
    spark : SparkSession
        A instância ativa do SparkSession que será usada para ler os dados.
    schema : StructType
        O schema que define a estrutura dos dados a serem lidos. Isso garante que os dados JSON sejam lidos com a tipagem correta.
    file_path : str
        O caminho completo do arquivo JSON que será lido.

    Retorno:
    DataFrame
        Um DataFrame PySpark contendo os dados lidos do arquivo JSON com o schema aplicado.
    """
    logging.info("Lendo dados em %s", file_path)

    df = spark.read.schema(schema)\
                   .option('multiline', 'true')\
                   .json(file_path)
    return df


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    date_now = datetime.date.today()
    logging.info(f"Iniciando a execução do dia {date_now}")
    env_vars = EnvironmentVariables()
    bronze_layer_path = env_vars.bronze_path
    silver_layer_path = env_vars.silver_path
    spark = create_spark_session()
    schema = get_schema()
    df_bronze = read_json_from_path(spark, schema, bronze_layer_path)

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
            .format('parquet')\
            .partitionBy('country') \
            .parquet(silver_layer_path)

    logging.info("Fim do processo.")

    spark.stop()


if __name__ == '__main__':
    run()
