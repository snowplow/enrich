#!/bin/bash

export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar

aws --endpoint-url=http://localhost:4566 kinesis create-stream --stream-name raw --shard-count 1 --region eu-central-1
aws --endpoint-url=http://localhost:4566 kinesis create-stream --stream-name enriched --shard-count 1 --region eu-central-1
aws --endpoint-url=http://localhost:4566 kinesis create-stream --stream-name bad-1 --shard-count 1 --region eu-central-1
