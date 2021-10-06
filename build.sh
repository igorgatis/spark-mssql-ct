#!/bin/bash

pushd "$( dirname "${BASH_SOURCE[0]}" )"

sudo docker build . -t spark-mssql-ct-sbt

sudo docker run -it --rm \
  -v $PWD:/app \
  -w /app \
  spark-mssql-ct-sbt \
  sbt package
