FROM quay.io/debezium/connect:3.0

RUN curl --http1.1 -k -o archive https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.3/archive \
    && unzip archive \
    && mv confluentinc-kafka-connect-s3-10.6.3 /kafka/connect