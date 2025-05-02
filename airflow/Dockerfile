FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install apache-airflow-providers-apache-spark minio


