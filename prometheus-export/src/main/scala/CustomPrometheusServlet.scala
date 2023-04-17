package org.apache.spark.metrics.sink

import java.util.Properties
import com.codahale.metrics.MetricRegistry
import javax.servlet.http.HttpServletRequest

/**
  * This class extends the PrometheusServlet class to implement custom exports of prometheus metrics.
  * See https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/metrics/sink/PrometheusServlet.scala
  * for details on the parent class implementation.
  * 
  * Without any overridden methods this class behaves like the parent class. To implement custom export
  * logic, the `getMetricsSnapshot` method must be overridden. The returned string will be returned
  * in the configured prometheus endpoint and should thus represent valid prometheus metrics.
  * 
  * The PrometheusServlet class can only be overridden by classes in the `org.apache.spark` package.
  * This is definitely not recommended practice when working with JVM/Scala classes and packages and
  * should be considered a hack.
  *
  * @param properties
  * @param registry
  */
class CustomPrometheusServlet(properties: Properties, registry: MetricRegistry) extends PrometheusServlet(properties, registry) {
  override def getMetricsSnapshot(request: HttpServletRequest): String = {
    "TODO"
  }
}