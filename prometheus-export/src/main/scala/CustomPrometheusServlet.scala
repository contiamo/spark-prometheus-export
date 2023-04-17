package org.apache.spark.metrics.sink

import java.util.Properties
import scala.collection.immutable.ListMap

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
  * This implementation tries to follow the Prometheus spec and best practices
  * as defined in https://prometheus.io/docs/instrumenting/exposition_formats/
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

  import CustomPrometheusServlet._

  override def getMetricsSnapshot(request: HttpServletRequest): String = {
    import scala.collection.JavaConverters._

    val sb = new StringBuilder()
    val metrics = registry
      .getMetrics()
      .entrySet()
      .iterator()
      .asScala
      .map(kv => {
        val value = kv.getValue
        value match {
          case c: Counter => {
            formatCounter(kv.getKey, c)
          }
          case g: Gauge[_] => {
            formatGauge(kv.getKey, g)
          }
          case m: Meter => {
            formatMeter(kv.getKey, m)
          }
          case h: Histogram => {
            formatHistogram(kv.getKey, h)
          }
          case t: Timer => {
            formatTimer(kv.getKey, t)
          }
        }
      })

    groupMetricValues(metrics).foreach(group => {
      val ((key, metricType), values) = group
      if (metricType != "untyped") {
        sb.append(s"# TYPE ${key} ${metricType}\n")
      }
      values.foreach(v => sb.append(s"${v}"))
      sb.append("\n")
    })

    sb.toString()
  }

  /** Group the metric values by the name and type, and return a map of the
    * metric name and type to a list of values concatenated together.
    *
    * Any metrics with an empty name are filtered out.
    *
    * Metrics are sorted by name.
    */
  private def groupMetricValues(
      metrics: Iterator[(String, String, String)]
  ): ListMap[(String, String), List[String]] = {
    val sortedMetrics = metrics.toSeq
      .filter { case (key, _, _) => key != null && key.nonEmpty }
      .sortBy { case (key, _, _) => key }

    sortedMetrics
      .foldLeft(ListMap.empty[(String, String), List[String]]) {
        case (acc, (key, metricType, value)) => {
          val keyType = (key, metricType)
          val values = acc.getOrElse(keyType, List[String]())
          acc + (keyType -> (value :: values))
        }
      }
  }

}

object CustomPrometheusServlet {

  /** Format a Gauge metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    */
  def formatGauge(k: String, v: Gauge[_]): (String, String, String) = {
    val numericValue = v.getValue match {
      case n: Int    => Some(n.toFloat)
      case n: Long   => Some(n.toFloat)
      case n: Float  => Some(n)
      case n: Double => Some(n.toFloat)
      case _         => None
    }

    numericValue match {
      case Some(numValue) =>
        val (key, labels) = parseMetricKey(k)
        val labelString = serializeLabels(labels)
        (key, "gauge", s"${key}${labelString} ${numValue}\n")
      case None =>
        ("", "", "")
    }
  }

  /** Format a Timer metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    */
  def formatCounter(k: String, v: Counter): (String, String, String) = {
    val (key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels)
    (key, "counter", s"${key}_total${labelString} ${v.getCount}\n")
  }

  /** Format a Histogram as a Summary metric in the Prometheus metric string,
    * returns the metric name and the text formatted value. This can be used to
    * group and format multiple metrics into a single metric.
    */
  def formatHistogram(
      k: String,
      v: Histogram
  ): (String, String, String) = {
    val (key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels)

    val sb = new StringBuilder()
    val sum = snapshot.getValues().sum
    sb.append(s"${key}_count${labelString} ${v.getCount}\n")
    sb.append(s"${key}_sum${labelString} ${sum}\n")

    Seq(
      0.5, 0.75, 0.95, 0.98, 0.99, 0.999
    ).foreach(q => {
      val value = snapshot.getValue(q)
      val quartileLabels = labels + ("quantile" -> s"${q}")
      val quartileLabelString = serializeLabels(quartileLabels)
      sb.append(s"${key}${quartileLabelString} ${value}\n")
    })
    (key, "summary", sb.toString())
  }

  /** Format a Timer metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    *
    * Note that we ignore the Meter values in the Timer, because they are not
    * well supported in Prometheus and can be recovered from the rate function.
    */
  def formatTimer(k: String, v: Timer): (String, String, String) = {
    val (key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels)

    val sb = new StringBuilder()
    val sum = snapshot.getValues().sum
    sb.append(s"${key}_count${labelString} ${v.getCount}\n")
    sb.append(s"${key}_sum${labelString} ${sum}\n")

    Seq(
      0.5, 0.75, 0.95, 0.98, 0.99, 0.999
    ).foreach(q => {
      val value = snapshot.getValue(q)
      val quartileLabels = labels + ("quantile" -> s"${q}")
      val quartileLabelString = serializeLabels(quartileLabels)
      sb.append(s"${key}${quartileLabelString} ${value}\n")
    })
    (key, "summary", sb.toString())
  }

  /** Format a Meter metric as a UNTYPED metric with labels for the mean rate
    * and the 1 minute rate, and 5 minutes rate this is not ideal, but preserves
    * the information.
    */
  def formatMeter(k: String, v: Meter): (String, String, String) = {
    val (key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels)

    val sb = new StringBuilder()
    sb.append(s"${key}_count${labelString} ${v.getCount}\n")

    val labelsOneMin = serializeLabels(labels + ("rate" -> "1m"))
    sb.append(s"${key}_rate${labelsOneMin} ${v.getOneMinuteRate}\n")

    val labelsFiveMin = serializeLabels(labels + ("rate" -> "5m"))
    sb.append(s"${key}_rate${labelsFiveMin} ${v.getFiveMinuteRate}\n")

    val labelsFifteenMin =
      serializeLabels(labels + ("rate" -> "15m"))
    sb.append(s"${key}_rate${labelsFifteenMin} ${v.getFifteenMinuteRate}\n")

    (key, "untyped", sb.toString())
  }

  /** normalize a metric name by removing all non-alphanumeric characters
    * replace them with underscores.
    */
  def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}"
  }

  /** Serialize a map of labels into a Prometheus label string.
    */
  def serializeLabels(labels: Map[String, String]): String = {
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
    val parts = input.split(".LABELS.")
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
