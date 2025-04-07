
===============================================================MAIN======================================================================
docker exec -it mysql mysql -u lminhnguyet -p -D pixar_films
show tables;
select * from films;

docker exec -it minio bash -c "mc alias set minio http://minio:9000 minio minio123 && mc mb minio/pixarfilmskafka"

curl -H "Accept:application/json" localhost:8083/connectors/
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @connector/conf/mysql-connector.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @connector/conf/minio-connector.json
curl -X DELETE localhost:8083/connectors/minio-connector

docker exec -it kafka bash -c "bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group connect-minio-connector --delete"

docker exec -it spark-master bash

spark-submit /app/src/full_load.py

docker exec -u root -it spark-master bin/spark-submit /app/stream.py

-- mỗi ngày chạy 1 script insert/update vài record cho mysql

NOTE:
1. Vấn đề small file:
- flush.size & rotate.interval.ms bằng bao nhiêu thì hợp lý? (1 file nên chứa bao nhiêu record)

===========================================================================================================================================
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