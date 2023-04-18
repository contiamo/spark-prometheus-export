# COPY THIS BUILD STEP
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.8.2_2.12.17 AS metricsbuilder

WORKDIR /project
RUN mkdir project
RUN echo "sbt.version=1.8.2" > ./project/build.properties
COPY build.sbt ./
COPY prometheus-export ./prometheus-export

RUN sbt 'project prometheusExport' package

FROM scratch as distribution

COPY --from=metricsbuilder /project/prometheus-export/target/scala-2.12/*.jar /jars/

FROM apache/spark-py:v3.3.2

# USE THIS LINE IN YOUR DOCKERFILE
COPY --from=metricsbuilder /project/prometheus-export/target/scala-2.12/*.jar /opt/spark/jars

CMD /opt/spark/bin/pyspark \
  --conf spark.ui.prometheus.enabled=true \
  --conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.CustomPrometheusServlet \
  --conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus
