FROM docker.io/bitnami/spark:3.5

USER root

COPY ./jars/*.jar /opt/bitnami/spark/jars

RUN export PYSPARK_SUBMIT_ARGS="--master spark://spark-master:7077 pyspark-shell"
RUN pip install minio