#!/bin/bash
# Run after Kafka container is up

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic geopolitical-events \
  --partitions 5 \
  --replication-factor 1 \
  --config retention.ms=604800000

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic market-data \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "Topics created successfully"
