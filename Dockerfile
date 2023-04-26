# COPY THIS BUILD STEP
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.8.2_2.12.17 AS metricsbuilder

WORKDIR /project
RUN mkdir project
COPY project/build.properties ./project
COPY build.sbt ./
COPY prometheus-export ./prometheus-export

RUN sbt 'project prometheusExport' package

FROM scratch as distribution

COPY --from=metricsbuilder /project/prometheus-export/target/scala-2.12/*.jar /jars/
