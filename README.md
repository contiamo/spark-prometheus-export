# Pyspark Metrics Export

This sbt/scala project provides an override of the default spark prometheus exporter to support proper naming and labels and a spark stream listener to track progress metrics. Both components can be used with either spark or pyspark.

__NOTE: The implementation extends private classes in the spark packages. This should be considered a hack and is fragile. Any changes to the class in Spark might break this solution.__

## Quick Start

### Docker

The Jar can be extracted from the published docker image. In the docker file for your project add the `eu.gcr.io/contiamo-public/spark-prometheus-export:{VERSION}` image as a stage and copy the Jar file from there.

```
FROM eu.gcr.io/contiamo-public/spark-prometheus-export:{VERSION} AS exporter-jars

FROM apache/spark-py:latest

COPY --from=exporter-jars /jars/*.jar /opt/spark/jars/
```

### Jar File

It is possible to use the project's Jar file directly. To do this, download the Jar from the latest [release](https://github.com/contiamo/spark-prometheus-export/releases) and make it available to spark in the class path. This can be done by copying the jar into the `jars` directory of the spark installation.

## Build the Jar

To use the implementation in this repository in a spark job, you have to build the jar, put it into the classpath of your spark job and reference the custom servlet via the `{...}.servlet.class` config option.

Building the jar can be done via sbt:
```bash
# task build
sbt 'project prometheusExport' package
```
This will build the code and package it into the the jar file `./target/scala-2.12/prom-servlet_2.12-{VERSION}.jar`.

The created jar can then be used in a spark job. The following snippet starts a pyspark REPL with the jar used to update the prometheus export.
```bash
task docker:build
docker run --rm -it \
  -v $(pwd)/prometheus-export/target/scala-2.12/prom-servlet_2.12-0.0.1.jar:/opt/spark/jars/prom-servlet_2.12-0.0.1.jar \
  -p 4040:4040 \
  apache/spark-py:v3.3.2 \
  /opt/spark/bin/pyspark \
  --conf spark.ui.prometheus.enabled=true \
  --conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.CustomPrometheusServlet \
  --conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus
```
With the REPL running the custom metrics will then be reported at `localhost:4040/metrics/prometheus`.
