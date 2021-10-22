#!/bin/bash

set -x

export PYTHONWARNINGS="ignore:Unverified HTTPS request"

region=eu-central-1
collectorPayloadsStream=hackathon-collector-payloads
enrichedStream=hackathon-enriched
badRowsStream=hackathon-bad

aws --region $region kinesis create-stream --stream-name $collectorPayloadsStream --shard-count 1 
aws --region $region kinesis create-stream --stream-name $enrichedStream --shard-count 1 
aws --region $region kinesis create-stream --stream-name $badRowsStream --shard-count 1 

sbt "project kinesis" assembly
target="modules/kinesis/target/scala-2.12/"
JAR=$target$(ls -1rth $target | tail -1)
java -jar $JAR --config /home/ben/code/enrich/test/hackathon.hocon --iglu-config /home/ben/code/enrich/test/iglu_resolver.json --enrichments /home/ben/code/enrich/test/enrichments/ > enrich-kinesis.log 2>&1 &
pid=$!

sbt "project kinesisIntegrationTests" assembly
target="modules/kinesis-integration-tests/target/scala-2.12/"
JAR=$target$(ls -1rth $target | tail -1)
java -jar $JAR $region $collectorPayloadsStream $enrichedStream $badRowsStream
res=$?

kill $pid
aws --region $region kinesis delete-stream --stream-name $collectorPayloadsStream --enforce-consumer-deletion
aws --region $region kinesis delete-stream --stream-name $enrichedStream --enforce-consumer-deletion
aws --region $region kinesis delete-stream --stream-name $badRowsStream --enforce-consumer-deletion
aws --region $region dynamodb delete-table --table-name enrich-kinesis-integration-tests
aws --region $region dynamodb delete-table --table-name snowplow-enrich-kinesis

exit $res
