FROM apache/airflow:2.5.1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# para fazer o pyspark funcionar no docker com airflow
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

ENV BRONZE="/opt/airflow/data/1_bronze/"
ENV SILVER="/opt/airflow/data/2_silver"
ENV GOLD="/opt/airflow/data/3_gold/tb_brewery_data"

USER airflow