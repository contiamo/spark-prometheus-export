package org.apache.spark.metrics.sink

import java.util.Properties
import com.codahale.metrics.MetricRegistry
import javax.servlet.http.HttpServletRequest
import com.codahale.metrics.Gauge
import com.codahale.metrics.Counter
import com.codahale.metrics.Histogram
import com.codahale.metrics.Meter
import com.codahale.metrics.Timer

/** This class extends the PrometheusServlet class to implement custom exports
  * of prometheus metrics. See
  * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/metrics/sink/PrometheusServlet.scala
  * for details on the parent class implementation.
  *
  * Without any overridden methods this class behaves like the parent class. To
  * implement custom export logic, the `getMetricsSnapshot` method must be
  * overridden. The returned string will be returned in the configured
  * prometheus endpoint and should thus represent valid prometheus metrics.
  *
  * The PrometheusServlet class can only be overridden by classes in the
  * `org.apache.spark` package. This is definitely not recommended practice when
  * working with JVM/Scala classes and packages and should be considered a hack.
  *
  * @param properties
  * @param registry
  */
class CustomPrometheusServlet(properties: Properties, registry: MetricRegistry)
    extends PrometheusServlet(properties, registry) {

  override def getMetricsSnapshot(request: HttpServletRequest): String = {
    import scala.collection.JavaConverters._

    val countersLabel = """{type="counters"}"""
    val metersLabel = countersLabel
    val histogramslabels = """{type="histograms"}"""
    val timersLabels = """{type="timers"}"""

    val sb = new StringBuilder()
    registry
      .getMetrics()
      .entrySet()
      .iterator()
      .asScala
      .foreach(kv => {
        val value = kv.getValue
        value match {
          case c: Counter => {
            appendCounterMetric(sb, kv.getKey, c)
          }
          case m: Meter => {
            appendMeterMetric(sb, kv.getKey, m)
          }
          case h: Histogram => {
            appendHistogramMetric(sb, kv.getKey, h)
          }
          case t: Timer => {
            appendTimerMetric(sb, kv.getKey, t)
          }
          case g: Gauge[_] => {
            appendGaugeMetrics(sb, kv.getKey, g)
          }
          case _ => {
            // do nothing, maybe debug log something?
          }
        }
      })

    sb.toString()
  }

  /** Append a Gauge metric to the Prometheus metric string
    */
  private def appendGaugeMetrics(
      sb: StringBuilder,
      k: String,
      v: Gauge[_]
  ): Unit = {
    if (v.getValue.isInstanceOf[String]) {
      return
    }

    val (key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels + ("type" -> "gauges"))

    sb.append(s"${key}Number$labelString ${v.getValue}\n")
    sb.append(s"${key}Value$labelString ${v.getValue}\n")
  }

  /** Append a Counter metric to the Prometheus metric string
    */
  private def appendCounterMetric(
      sb: StringBuilder,
      k: String,
      v: Counter
  ): Unit = {
    val (key, labels) = parseMetricKey(k)

    val labelString = serializeLabels(labels + ("type" -> "counters"))
    sb.append(s"${key}Count$labelString ${v.getCount}\n")
  }

  /** Append a Histogram metric to the Prometheus metric string
    */
  private def appendHistogramMetric(
      sb: StringBuilder,
      k: String,
      v: Histogram
  ): Unit = {
    val (key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels + ("type" -> "histograms"))
    sb.append(s"${key}Count$labelString ${v.getCount}\n")
    sb.append(s"${key}Max$labelString ${snapshot.getMax}\n")
    sb.append(s"${key}Mean$labelString ${snapshot.getMean}\n")
    sb.append(s"${key}Min$labelString ${snapshot.getMin}\n")
    sb.append(s"${key}50thPercentile$labelString ${snapshot.getMedian}\n")
    sb.append(
      s"${key}75thPercentile$labelString ${snapshot.get75thPercentile}\n"
    )
    sb.append(
      s"${key}95thPercentile$labelString ${snapshot.get95thPercentile}\n"
    )
    sb.append(
      s"${key}98thPercentile$labelString ${snapshot.get98thPercentile}\n"
    )
    sb.append(
      s"${key}99thPercentile$labelString ${snapshot.get99thPercentile}\n"
    )
    sb.append(
      s"${key}999thPercentile$labelString ${snapshot.get999thPercentile}\n"
    )
    sb.append(s"${key}StdDev$labelString ${snapshot.getStdDev}\n")
  }

  /** Append a Meter as a counter metric to the Prometheus metrics string.
    */
  private def appendMeterMetric(
      sb: StringBuilder,
      k: String,
      v: Meter
  ): Unit = {
    val (key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels + ("type" -> "counters"))
    sb.append(s"${key}Count$labelString ${v.getCount}\n")
    sb.append(s"${key}MeanRate$labelString ${v.getMeanRate}\n")
    sb.append(s"${key}OneMinuteRate$labelString ${v.getOneMinuteRate}\n")
    sb.append(s"${key}FiveMinuteRate$labelString ${v.getFiveMinuteRate}\n")
    sb.append(
      s"${key}FifteenMinuteRate$labelString ${v.getFifteenMinuteRate}\n"
    )
  }

  /** Append a Timer metric to the Prometheus metric string
    */
  private def appendTimerMetric(
      sb: StringBuilder,
      k: String,
      v: Timer
  ): Unit = {
    val (key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels + ("type" -> "timers"))
    sb.append(s"${key}Count$labelString ${v.getCount}\n")
    sb.append(s"${key}Max$labelString ${snapshot.getMax}\n")
    sb.append(s"${key}Mean$labelString ${snapshot.getMean}\n")
    sb.append(s"${key}Min$labelString ${snapshot.getMin}\n")
    sb.append(s"${key}50thPercentile$labelString ${snapshot.getMedian}\n")
    sb.append(
      s"${key}75thPercentile$labelString ${snapshot.get75thPercentile}\n"
    )
    sb.append(
      s"${key}95thPercentile$labelString ${snapshot.get95thPercentile}\n"
    )
    sb.append(
      s"${key}98thPercentile$labelString ${snapshot.get98thPercentile}\n"
    )
    sb.append(
      s"${key}99thPercentile$labelString ${snapshot.get99thPercentile}\n"
    )
    sb.append(
      s"${key}999thPercentile$labelString ${snapshot.get999thPercentile}\n"
    )
    sb.append(s"${key}StdDev$labelString ${snapshot.getStdDev}\n")
    sb.append(
      s"${key}FifteenMinuteRate$labelString ${v.getFifteenMinuteRate}\n"
    )
    sb.append(s"${key}FiveMinuteRate$labelString ${v.getFiveMinuteRate}\n")
    sb.append(s"${key}OneMinuteRate$labelString ${v.getOneMinuteRate}\n")
    sb.append(s"${key}MeanRate$labelString ${v.getMeanRate}\n")
  }

  /** normalize a metric name by removing all non-alphanumeric characters
    * replace them with underscores.
    */
  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  /** Serialize a map of labels into a Prometheus label string.
    */
  private def serializeLabels(labels: Map[String, String]): String = {
    labels
      .map { case (k, v) => s"""$k="$v"""" }
      .mkString("{", ",", "}")
  }

  /** Parses an input string representing a metric key and its labels, and
    * returns a tuple containing the key and a map of the labels.
    *
    * The input string is expected to have the format
    * "metric.key.LABELS.label_key1.label_val1.label_key2.label_val2...", where
    * the "metric.key" value is a string representing the metric name or
    * category, and the "LABELS" string is used to indicate the start of the
    * label section. The label section consists of a variable number of
    * key-value pairs separated by dots. Empty labels, i.e. those with empty
    * label keys or values, are ignored.
    *
    * Any trailing dots in the "metric.key" section of the input string are
    * automatically removed before the result is returned.
    *
    * @param input
    *   the input string to parse
    * @return
    *   a tuple containing the key string and a map of the labels
    */
  def parseMetricKey(input: String): (String, Map[String, String]) = {
    val parts = input.split("LABELS")
    var key = parts(0)
    while (key.endsWith(".")) {
      key = key.dropRight(1)
    }
    var labels = Map.empty[String, String]
    if (parts.length > 1) {
      val labelParts = parts(1).split("\\.")
      labelParts.grouped(2).foreach {
        case Array(k, v) if k.nonEmpty && v.nonEmpty =>
          labels += k -> v
        case _ =>
      }
    }
    (normalizeKey(key), labels)
  }
}
