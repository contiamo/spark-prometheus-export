package org.apache.spark.metrics.source

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession
import com.codahale.metrics.{MetricRegistry, Histogram, SettableGauge}

import java.time.Duration
import java.util.UUID

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.internal.Logging
import com.codahale.metrics.Gauge

object StreamingQuerySource extends Logging {
  def metricWithLabels(name: String, labels: Map[String, String]): String = {
    if (labels.isEmpty)
      name
    else
      s"$name.LABELS.${labels.map { case (key, value) => s"$key.$value" }.mkString(".")}"
  }

  /** extract the topic subscription from the description for example
    * "KafkaV2[Subscribe[mpathic-event]]" becomes "mpathic-event"
    */
  def extractSubscriptionName(value: String): String = {
    value.split("\\[").last.split("\\]").head
  }

  /** extract the source name from the description for example
    * "KafkaV2[Subscribe[mpathic-event]]" becomes "KafkaV2"
    */
  def extractSourceName(value: String): String = {
    value.split("\\[").head
  }

  // An offset object is a json field with a map of topics to a map of partitions to offsets
  // for example
  // {
  //     "mpathic-event": {
  //         "0": 689125037,
  //         "1": 597416917,
  //         "2": 589446933,
  //         "3": 444659423,
  //         "4": 432998635,
  //         "5": 429711474,
  //         "6": 445322191,
  //         "7": 432412809,
  //         "8": 431092969,
  //         "9": 444619591
  //     }
  //     "topic2": {
  //         "0": 689125037,
  //         "1": 597416917,
  //         "2": 589446933,
  //         "3": 444659423,
  //         "4": 432998635
  //     }
  // }
  def getOffset(
      offsetJSON: String
  ): Map[String, Map[String, Long]] = {
    if (offsetJSON == null || offsetJSON == "") {
      return Map()
    }

    offsetJSON.trim.headOption match {
      case Some('{') if offsetJSON.trim.lastOption.contains('}') =>
        implicit val formats = DefaultFormats
        parse(offsetJSON).extract[Map[String, Map[String, Long]]]
      case _ =>
        log.trace(s"Streaming offset data is not processable: $offsetJSON")
        Map()
    }
  }

}

class StreamingQuerySource() extends Source with Logging {

  import StreamingQuerySource._

  val registry: MetricRegistry = new MetricRegistry()
  def sourceName(): String = "StreamingQuerySource"

  def metricRegistry(): MetricRegistry = registry

  def reportGauge(
      name: String,
      labels: Map[String, String],
      value: Long
  ): Unit = {
    log.trace(s"Reporting gauge $name with labels $labels and value $value")
    val m = registry.gauge[SettableGauge[Long]](metricWithLabels(name, labels))
    m.setValue(value)
  }

  def reportHistogram(
      name: String,
      labels: Map[String, String],
      value: Long
  ): Unit = {
    log.trace(s"Reporting histogram $name with labels $labels and value $value")
    val meter = registry.histogram(metricWithLabels(name, labels))
    meter.update(value)
  }

  /** Process the progress object and report metrics to the registry
    *
    * We want to use the `name` as a label in each metric we create
    *
    * Here are the metrics we want to create. At the batch level, meaning each
    * metric should have the prefix streaming.batch: \1. create histogram
    * metrics for numInputRows, we prefer a histogram because it provides a
    * history of size for every batch, not just the last batch 2. create a timer
    * named duration_ms for each value in the durationMs object, the name of the
    * key should be a label on the metric
    *
    * We then want to create metrics for each source in `sources` list named
    * streaming.source.kafka.partition.offset for each partition in startOffset,
    * endOffset, latestOffset, we want to create gauge metrics with labels for
    * the topic, the partition number, and the offset type (start, end, latest)
    *
    * We also want to create a histogram metric for each source for the
    * numInputRows streaming.source.kafka.rows this should also have the topic
    * as a label.
    *
    * @param progress
    */
  def reportProgressMetrics(progress: StreamingQueryProgress): Unit = {
    val commonLabels = Map("name" -> progress.name)

    log.info(s"Reporting metrics for ${progress.name}")

    reportGauge(
      "streaming.batch.num_input_rows",
      commonLabels,
      progress.numInputRows
    )

    reportHistogram(
      "streaming.batch.duration_ms",
      commonLabels,
      progress.batchDuration
    )

    for ((key, value) <- progress.durationMs.asScala) {
      reportHistogram(
        s"streaming.batch.duration_ms",
        commonLabels + ("duration" -> key),
        value
      )
    }

    for (source <- progress.sources) {
      val name = extractSourceName(source.description)
      name match {
        case "KafkaV2" => {
          val subscription = extractSubscriptionName(source.description)
          reportHistogram(
            "streaming.source.kafka.rows",
            commonLabels + ("subscription" -> subscription, "source" -> name),
            source.numInputRows
          )

          val endOffset = getOffset(source.endOffset)
          for ((topic, partitions) <- endOffset) {
            for ((partition, offset) <- partitions) {
              reportGauge(
                s"streaming.source.kafka.partition.offset",
                commonLabels + ("topic" -> topic, "partition" -> partition, "offset_type" -> "endOffset"),
                offset
              )
            }
          }

          val startOffset = getOffset(source.startOffset)
          for ((topic, partitions) <- startOffset) {
            for ((partition, offset) <- partitions) {
              reportGauge(
                s"streaming.source.kafka.partition.offset",
                commonLabels + ("topic" -> topic, "partition" -> partition, "offset_type" -> "startOffset"),
                offset
              )
            }
          }

          val latestOffset = getOffset(source.endOffset)
          for ((topic, partitions) <- latestOffset) {
            for ((partition, offset) <- partitions) {
              reportGauge(
                s"streaming.source.kafka.partition.offset",
                commonLabels + ("topic" -> topic, "partition" -> partition, "offset_type" -> "latestOffset"),
                offset
              )
            }
          }
        }
        // TODO: support other sources
        case _ => None
      }

    }
  }

  def start() = {
    val spark = SparkSession.active
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(
          event: StreamingQueryListener.QueryStartedEvent
      ): Unit = {
        reportGauge(
          "streaming.active_streams",
          Map.empty,
          spark.streams.active.length
        )
      }

      override def onQueryTerminated(
          event: StreamingQueryListener.QueryTerminatedEvent
      ): Unit = {
        reportGauge(
          "streaming.active_streams",
          Map.empty,
          spark.streams.active.length
        )
      }

      override def onQueryProgress(
          event: StreamingQueryListener.QueryProgressEvent
      ): Unit = {
        reportProgressMetrics(event.progress)
      }
    })
  }
}
