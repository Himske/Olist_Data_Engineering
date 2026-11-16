FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.9.3" "pyspark==3.5.1"

USER root
RUN ln -s /home/airflow/.local/lib/python3.11/site-packages/pyspark /opt/spark
USER airflow
ENV SPARK_HOME=/opt/spark
