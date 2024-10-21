import datetime
import logging

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


def read_parquet_from_path(spark: SparkSession, schema: StructType, file_path: str, cols: list, format='parquet') -> DataFrame:
    """
    Lê dados em parquet a partir de um caminho fornecido utilizando o Spark.

    Argumentos:
    spark : SparkSession
        A instância ativa do SparkSession que será usada para ler os dados.
    schema : pyspark.sql.types.StructType
        O schema que define a estrutura dos dados a serem lidos.
    file_path : str
        O caminho completo do arquivo ou diretório onde o(s) arquivo(s) Parquet (ou outro formato) está(ão) armazenado(s).
    cols : list, opcional
        Lista de colunas a serem selecionadas a partir do DataFrame resultante.
    format : str, opcional
        O formato do arquivo a ser lido, o padrão é 'parquet'.

    Retorno:
    DataFrame
        Um DataFrame PySpark contendo os dados lidos e as colunas selecionadas (se fornecidas).
    """
    logging.info("Lendo dados em %s", file_path)
    df = spark.read.schema(schema)\
        .format(format)\
        .load(file_path)\
        .select(cols)
    logging.info("Foram lidos %i registros.", df.count())

    return df


def create_aggregated_view(df: DataFrame) -> DataFrame:
    """
    Cria uma visão agregada de um DataFrame agrupando os dados por país e tipo de cervejaria, e contando o número de ocorrências.

    Argumentos:
    df : DataFrame
        O DataFrame PySpark contendo os dados de entrada, que deve incluir as colunas 'country' (país) e 'brewery_type' (tipo de cervejaria)

    Retorno:
    DataFrame
        Um DataFrame PySpark contendo a contagem de registros, agrupado pelas colunas 'country' e 'brewery_type'
    """
    df_agg = df.groupBy("country", "brewery_type").count()
    logging.info("A view gerada contém %i registros", df_agg.count())
    return df_agg


def run():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Iniciando a execução do dia {datetime.date.today()}")
    env_vars = EnvironmentVariables()
    silver_layer_path = env_vars.silver_path
    gold_layer_path = env_vars.gold_path
    spark = create_spark_session()
    schema = get_schema()
    df = read_parquet_from_path(spark, schema, silver_layer_path, [
                                "country", "brewery_type"])
    df_agg = create_aggregated_view(df)
    logging.info(df_agg.show(20))
    logging.info("Salvando a view gerada...")
    df_agg.write\
        .mode("overwrite") \
        .format('parquet')\
        .parquet(gold_layer_path)
    logging.info("Fim do processo.")

    spark.stop()


if __name__ == '__main__':
    run()
