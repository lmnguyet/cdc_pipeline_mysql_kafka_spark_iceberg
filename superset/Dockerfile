FROM apache/superset:latest

USER root

RUN pip install trino

COPY ./superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset

ENTRYPOINT ["bash", "-c", "\
  superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin && \
  superset db upgrade && \
  superset init && \
  /usr/bin/run-server.sh\
"]