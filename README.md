# Pyspark Metrics Export

This sbt/scala project provides a minimal template to extending the default prometheus export of spark.

__NOTE: The custom servlet class extends the package private `PrometheusServlet` class. This should be considered a hack and is fragile. Any changes to the class in Spark might break this solution.__

## Using the custom export in a spark job

To use the implementation in this repository in a spark job, you have to build the jar, put it into the classpath of your spark job and reference the custom servlet via the `{...}.servlet.class` config option.

Building the jar can be done via sbt:
```bash
sbt 'project prometheusExport' package
```
This will build the code and package it into the the jar file `./target/scala-2.12/prom-servlet_2.12-{VERSION}.jar`.

The created jar can then be used in a spark job. The following snippet starts a pyspark REPL with the jar used to update the prometheus export.
```bash
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

## Embedding into an existing pyspark project

Since the code relies in spark implementation details and should generally be considered unstable, we do not recommend publishing the generated artifact to a package repository. Instead, this repository is stuctured in a way that should make it easy to copy the implementation to your pyspark project.

This requires three additions to your project structure:
1. Copy the `prometheus-export` directory to your project. The directory contains the sources for the custom servlet.
2. Add the contents of the [`build.sbt`](./build.sbt) file to your `build.sbt` file. If your project doesn't have a `build.sbt` file, simply copy the one from this repo.
2. Add the build step in the [`Dockerfile`](./Dockerfile) of this repo to your own `Dockerfile` and make sure to copy the generated jars to your final image.