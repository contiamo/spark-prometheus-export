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

    groupMetricValues(metrics).foreach({
      case MetricsRefGroup(key, metricType, values) =>
        if (metricType != "untyped") {
          sb.append(s"# TYPE ${key} ${metricType}\n")
        }
        values.foreach(sb.append(_))
        sb.append("\n")
    })

    sb.toString()
  }
}

object CustomPrometheusServlet {
  // prometheus labels
  type Labels = Map[String, String]
  // a MetricsKey contains the base key and all labels
  case class MetricsKey(key: String, labels: Labels)
  // reference to a single metrics value
  case class MetricsRef(key: String, tpe: String, formattedValue: String)
  // reference to a group of values under the same key
  case class MetricsRefGroup(key: String, tpe: String, values: List[String])

  /** Format a Gauge metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    */
  def formatGauge(k: String, v: Gauge[_]): MetricsRef = {
    val numericValue = v.getValue match {
      case n: Int    => Some(n.toFloat)
      case n: Long   => Some(n.toFloat)
      case n: Float  => Some(n)
      case n: Double => Some(n.toFloat)
      case _         => None
    }

    numericValue match {
      case Some(numValue) =>
        val MetricsKey(key, labels) = parseMetricKey(k)
        val labelString = serializeLabels(labels)
        MetricsRef(key, "gauge", s"${key}${labelString} ${numValue}\n")
      case None =>
        MetricsRef("", "", "")
    }
  }

  /** Format a Timer metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    */
  def formatCounter(k: String, v: Counter): MetricsRef = {
    val MetricsKey(key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels)
    MetricsRef(key, "counter", s"${key}_total${labelString} ${v.getCount}\n")
  }

  // Available buckets for Dropwizards metrics snapshots.
  val Buckets = Seq(0.5, 0.75, 0.95, 0.98, 0.99, 0.999)

  /** Format a Histogram as a Summary metric in the Prometheus metric string,
    * returns the metric name and the text formatted value. This can be used to
    * group and format multiple metrics into a single metric.
    */
  def formatHistogram(
      k: String,
      v: Histogram
  ): MetricsRef = {
    val MetricsKey(key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels)

    val sum = snapshot.getValues().sum

    val bucketsString =
      Buckets
        .map { q =>
          val value = snapshot.getValue(q)
          val quartileLabels = labels + ("quantile" -> s"${q}")
          val quartileLabelString = serializeLabels(quartileLabels)
          s"${key}${quartileLabelString} ${value}"
        }
        .mkString("\n")

    val metricsString =
      s"""${key}_count${labelString} ${v.getCount}
         |${key}_sum${labelString} ${sum}
         |${bucketsString}
         |""".stripMargin

    MetricsRef(key, "summary", metricsString)
  }

  /** Format a Timer metric to the Prometheus metric string, returns the metric
    * name and the text formatted value. This can be used to group and format
    * multiple metrics into a single metric.
    *
    * Note that we ignore the Meter values in the Timer, because they are not
    * well supported in Prometheus and can be recovered from the rate function.
    */
  def formatTimer(k: String, v: Timer): MetricsRef = {
    val MetricsKey(key, labels) = parseMetricKey(k)
    val snapshot = v.getSnapshot
    val labelString = serializeLabels(labels)

    val sum = snapshot.getValues().sum

    val bucketsString =
      Buckets
        .map { q =>
          val value = snapshot.getValue(q)
          val quartileLabels = labels + ("quantile" -> s"${q}")
          val quartileLabelString = serializeLabels(quartileLabels)
          s"${key}${quartileLabelString} ${value}"
        }
        .mkString("\n")

    val metricsString =
      s"""${key}_count${labelString} ${v.getCount}
         |${key}_sum${labelString} ${sum}
         |${bucketsString}
         |""".stripMargin

    MetricsRef(key, "summary", metricsString)
  }

  /** Format a Meter metric as a UNTYPED metric with labels for the mean rate
    * and the 1 minute rate, and 5 minutes rate this is not ideal, but preserves
    * the information.
    */
  def formatMeter(k: String, v: Meter): MetricsRef = {
    val MetricsKey(key, labels) = parseMetricKey(k)
    val labelString = serializeLabels(labels)

    val labelsOneMin = serializeLabels(labels + ("rate" -> "1m"))
    val labelsFiveMin = serializeLabels(labels + ("rate" -> "5m"))
    val labelsFifteenMin =
      serializeLabels(labels + ("rate" -> "15m"))

    val metricsString =
      s"""${key}_count${labelString} ${v.getCount}
         |${key}_rate${labelsOneMin} ${v.getOneMinuteRate}
         |${key}_rate${labelsFiveMin} ${v.getFiveMinuteRate}
         |${key}_rate${labelsFifteenMin} ${v.getFifteenMinuteRate}
         |""".stripMargin

    MetricsRef(key, "untyped", metricsString)
  }

  /** normalize a metric name by removing all non-alphanumeric characters
    * replace them with underscores.
    */
  def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}"
  }

  /** Serialize a map of labels into a Prometheus label string.
    */
  def serializeLabels(labels: Labels): String = {
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
  def parseMetricKey(input: String): MetricsKey = {
    val parts = input.split(".LABELS.")
    var key = parts(0).replaceAll("(\\.)*$", "")

    if (parts.length > 1) {
      val labels = parts(1)
        .split("\\.")
        .grouped(2)
        .collect {
          case Array(k, v) if k.nonEmpty && v.nonEmpty => k -> v
        }
        .toMap

      MetricsKey(normalizeKey(key), labels)
    } else {
      MetricsKey(normalizeKey(key), Map.empty)
    }
  }

  /** Group the metric values by the name and type, and return a map of the
    * metric name and type to a list of values concatenated together.
    *
    * Any metrics with an empty name are filtered out.
    *
    * Metrics are sorted by name.
    */
  private def groupMetricValues(
      metrics: Iterator[MetricsRef]
  ): List[MetricsRefGroup] = {
    val sortedMetrics = metrics.toSeq
      .filter { ref => ref.key != null && ref.key.nonEmpty }
      .sortBy { ref => ref.key }(Ordering[String].reverse)

    sortedMetrics
      .foldLeft(List.empty[MetricsRefGroup]) {
        // Case 1: entry matches the current head -> prepend value in the current head
        case (headGroup :: tail, MetricsRef(key, tpe, value))
            if headGroup.key == key && headGroup.tpe == tpe =>
          headGroup.copy(values = value :: headGroup.values) :: tail
        // Case 2: entry does not match current head (or list is empty) -> prepend new entry to list
        case (list, MetricsRef(key, tpe, value)) =>
          MetricsRefGroup(key, tpe, List(value)) :: list
      }
  }
}
