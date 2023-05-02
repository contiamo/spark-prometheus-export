#!/bin/bash

docker build -t spark-prometheus-export-test .

cat metrics.txt | docker run -i spark-prometheus-export-test
