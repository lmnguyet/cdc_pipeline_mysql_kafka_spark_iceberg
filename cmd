
===============================================================MAIN======================================================================
source: mysql 8.0

zookeeper & kafka

debezium + kafka-connect-s3 connector

hive metastore + mysql + s3 storage

spark + s3 libraries + hive libraries + iceberg libraries

PIP INSTALL pyiceberg => spark, airflow

=========================================================================================================================================
docker exec -it mysql mysql -u lminhnguyet -p -D pixar_films
show tables;
select * from films;

docker exec -i mysql mysql -ulminhnguyet -p123 pixar_films < ./cdc.sql

INSERT INTO films VALUES (29, 'Inside Out 3', '2025-04-08', 90, 'PG', '');
INSERT INTO films VALUES (30, 'Inside Out 4', '2025-04-08', 100, 'PG', '');
INSERT INTO films VALUES (31, 'Inside Out 5', '2025-04-08', 120, 'PG', '');
UPDATE films SET run_time=95 WHERE film='Inside Out 5';
DELETE FROM films WHERE film='Inside Out 5';
UPDATE films SET plot='This is the final season of Inside Out' WHERE film='Inside Out 5';

docker exec -it minio bash -c "mc alias set minio http://minio:9000 minio minio123 && mc mb minio/pixarfilmskafka"

curl -H "Accept:application/json" localhost:8083/connectors/
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @connector/conf/mysql-connector.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @connector/conf/minio-connector.json
curl -X DELETE localhost:8083/connectors/minio-connector


docker exec -it kafka bash -c "bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group connect-minio-connector --delete"

docker exec -it spark-master spark-submit /app/src/incremental_load.py

spark-submit /app/src/bronze_full_load.py
spark-submit /app/src/bronze_incremental_load.py
spark-submit /app/src/silver_load.py

docker compose -f docker-compose.airflow.yml start
docker compose start mysql minio hive-metastore trino fast-api spark-master spark-worker
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler

docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" elasticsearch:6.8.13

trino://trino:@trino:8080/iceberg

curl -X POST "http://localhost:8000/query" \
-H "Content-Type: application/json" \
-d '{
    "sql": "SELECT d.film, f.budget FROM iceberg.default.slv_dim_films d INNER JOIN iceberg.default.slv_fact_box_office f on d.id = f.id ORDER BY f.budget DESC"
}' | jq

============================
docker start elasticsearch

jq -c '{ 
  "index": {
    "_index": ._index,
    "_id": ._id,
    "_type": ._type
  }
}, ._source' /mnt/c/Users/lminhnguyet/Downloads/students_anonymized.json > students_import1.json

curl -X POST "http://localhost:9200/_bulk" \
-H "Content-Type: application/x-ndjson" \
--data-binary "@students_import.json"

curl -X GET "http://localhost:9200/students/_count"

curl -X GET "http://localhost:9200/students/_search?size=5&pretty"

curl -X GET "http://localhost:9200/students/_search?pretty" \
-H 'Content-Type: application/json' \
-d '{
  "query": {
    "match": {
      "groups_name_with_format": "Group 1k"
    }
  }
}'

elasticdump \
  --input=data.json \
  --output=http://localhost:9200/my_index \
  --type=data

curl -XGET "http://localhost:9200/students/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "_source": false,
  "query": {
    "match_all": {}
  },
  "sort": [
    { "_id": "asc" }
  ],
  "size": 1000
}'
==========ELASTIC SEARCH=========================
1. Bulk API Import 
Phải convert file json ban đầu thành file json đúng format

jq -c '{ 
  "index": {
    "_index": ._index,
    "_id": ._id,
    "_type": ._type
  }
}, ._source' /mnt/c/Users/lminhnguyet/Downloads/students_anonymized.json > students_import.json

curl -X POST "http://localhost:9200/_bulk" \
-H "Content-Type: application/x-ndjson" \
--data-binary "@students_import.json"

2. elasticdump import
elasticdump --input=students_anonymized.json --output=http://localhost:9200/students --type=data

3. elasdump export
npm install -g elasticdump

elasticdump --input=http://localhost:9200/students --output=students_export_bulk.json --type=data
elasticdump --input=http://localhost:9200/students --output=students_export_dump.json --type=data
elasticdump --input=http://localhost:9200/students --output=students_export_stash.json --type=data

==========AIRFLOW=====================
export AIRFLOW_UID=$(id -u)

docker compose -f docker-compose.airflow.yml up

export AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077

pip uninstall -y apache-airflow


=================================
SELECT TBL_ID FROM TBLS WHERE TBL_NAME = 'slv_fact_box_office' AND DB_ID = 1;
SELECT SD_ID FROM TBLS WHERE TBL_ID = 25;
SELECT SERDE_ID FROM SDS WHERE SD_ID = 25;


DELETE FROM COLUMNS_V2 WHERE CD_ID IN (
    SELECT CD_ID FROM SDS WHERE SD_ID = 25
);
DELETE FROM TABLE_PARAMS WHERE TBL_ID = 25;
DELETE FROM SD_PARAMS WHERE SD_ID = 25;
DELETE FROM TBLS WHERE TBL_ID = 25;
DELETE FROM SDS WHERE SD_ID = 25;
DELETE FROM SERDES WHERE SERDE_ID = 25;


spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_dim_films
spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_dim_genres
spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_fact_film_genres
spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_fact_film_ratings
spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_fact_box_office

select count(1) from iceberg.default.slv_fact_film_ratings;

spark-submit /tmp/jobs/silver_incremental_load.py --tables slv_dim_films
SELECT COALESCE(source_snapshot_id, '0') AS source_snapshot_id
FROM iceberg.default.cfg_snapshots_tracking
WHERE sink_table = 'spark_catalog.default.slv_dim_films' AND source_table = 'spark_catalog.default.brz_films'
ORDER BY committed_at DESC
LIMIT 1

SELECT snapshot_id 
FROM iceberg.default.brz_dim_films.snapshots
ORDER BY committed_at DESC
LIMIT 1

===========================================================================================================================================
SELECT *
FROM iceberg.default."films$snapshots"
ORDER BY timestamp DESC;

SELECT * FROM TABLE(
  iceberg.snapshots_diff(
    'iceberg',
    'default',
    'films',
    5727751947216810520,
    7359423397521229960
  )
);

https://packages.confluent.io/maven/io/confluent/kafka-connect-s3/10.5.20/kafka-connect-s3-10.5.20.jar

docker exec -u root -it connect bash

curl http://localhost:8083/connector-plugins/ | grep "io.confluent.connect.s3.S3SinkConnector"
curl http://localhost:8083/connector-plugins/ | grep "io.debezium.connector.mysql.MySqlConnector"

KEY_CONVERTER

CONNECT_PLUGIN_PATH

export CONNECT_PLUGIN_PATH=/kafka/connect

echo $CONNECT_PLUGIN_PATH

kafka 3.9

docker exec -u root -it kafka-connector bash

wget --no-check-certificate https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.0/archive
wget --no-check-certificate https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive

curl --cacert /etc/ssl/cert.pem https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive
curl --cacert /etc/ssl/certs/ca-certificates.crt https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive
curl --capath /etc/ssl/certs/ https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive

curl -k -o archive https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive

yum install unzip

jar -xf archive

wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.0.8.Final/debezium-connector-mysql-3.0.8.Final-plugin.tar.gz

tar -xzf debezium-connector-mysql-3.0.8.Final-plugin.tar.gz
mv debezium-connector-mysql /usr/share/java/

chmod -R 755 /usr/share/java/debezium-connector-mysql

https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.0.8.Final/debezium-connector-mysql-3.0.8.Final-plugin.zip
https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.3.0.Final/debezium-connector-mysql-2.3.0.Final-plugin.zip

mysql 8.0
kafka 3.9.0
debezium-connector-mysql 2.3.0
kafka-connect-s3 10.6.0

curl -k --retry 3 --max-time 30 -o archive https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.0/archive

...
curl --http1.1 -k -o archive https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive
unzip archive
mv confluentinc-kafka-connect-s3-10.6.3 /kafka/connect

com.amazonaws.auth.DefaultAWSCredentialsProviderChain

bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group connect-minio-connector --describe

bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group connect-minio-connector --delete

bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group connect-minio-connector --topic dbserver1.pixar_films.films \
  --reset-offsets --to-earliest --execute

bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group connect-minio-connector \
  --topic dbserver1.pixar_films.films \
  --reset-offsets --to-earliest --execute

curl -H "Accept:application/json" localhost:8083/connectors/
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @mysql-connector.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @minio-connector.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @t.json
curl -X DELETE localhost:8083/connectors/minio-connector
curl -X DELETE localhost:8083/connectors/minio-connector1


,

      "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
      "path.format": "yyyy-MM-dd",
      "timestamp.extractor": "Record",
      "timestamp.field": "payload.ts_ms",
      "locale": "en-US",
      "timezone": "Asia/Ho_Chi_Minh",
      "partition.duration.ms": "86400000"

-- backup spark conf
spark.master                                        spark://spark-master:7077
spark.serializer                                    org.apache.spark.serializer.KryoSerializer
spark.executor.memory                               4g
spark.driver.memory                                 2g

spark.jars                                          /opt/bitnami/spark/jars/*
spark.sql.extensions                                io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                     org.apache.spark.sql.delta.catalog.DeltaCatalog

spark.hadoop.fs.s3a.endpoint                        http://minio:9000
spark.hadoop.fs.s3a.access.key                      minio
spark.hadoop.fs.s3a.secret.key                      minio123
spark.hadoop.fs.s3a.path.style.access               true
spark.hadoop.fs.s3a.connection.ssl.enabled          false
spark.hadoop.fs.s3a.impl                            org.apache.hadoop.fs.s3a.S3AFileSystem


docker exec -it spark-master /opt/bitnami/spark/bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,mysql:mysql-connector-java:8.0.28 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hive \
  --conf spark.sql.catalog.demo.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.demo.warehouse=s3a://warehouse/

CREATE TABLE demo.db.sample (
  id bigint,
  data string,
  category string)
USING iceberg
PARTITIONED BY (category);

INSERT INTO demo.db.sample VALUES 
(1, 'data1', 'cat1'),
(2, 'data2', 'cat2'),
(3, 'data3', 'cat1');

SELECT * FROM demo.db.sample;

docker run -d --platform linux/amd64 -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:4.0.0

docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'

docker run -d -p 9083:9083 --env SERVICE_NAME=metastore --env DB_DRIVER=postgres \  
     --env SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=hive" \  
     --mount source=warehouse,target=/opt/hive/data/warehouse \  
     --mount type=bind,source=`mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar,target=/opt/hive/lib/postgres.jar \  
     --name metastore-standalone apache/hive:4.0.0