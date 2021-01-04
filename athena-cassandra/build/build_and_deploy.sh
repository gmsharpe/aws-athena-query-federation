#!/bin/bash
#  from ${project_home}, run athena-cassandra/build/./build_and_deploy.sh
docker build -t cassandra-cdk-build:0.0.1  --file athena-cassandra/docker/Dockerfile.build .
docker run -it --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -v F:\\git-gms\\aws-athena-query-federation\\athena-cassandra\\cdk_deploy:/cdk_deploy cassandra-cdk-build:0.0.1
