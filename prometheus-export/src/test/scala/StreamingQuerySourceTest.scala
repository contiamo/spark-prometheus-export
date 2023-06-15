import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.metrics.source.StreamingQuerySource

class StreamingQuerySourceSpec extends AnyFlatSpec with Matchers {
  "metricWithLabels" should "return the metric name with labels" in {
    val name = "my_metric"
    val labels = Map("foo" -> "bar", "baz" -> "qux")
    val expectedOutput = "my_metric.LABELS.foo.bar.baz.qux"
    val actualOutput = StreamingQuerySource.metricWithLabels(name, labels)
    actualOutput shouldEqual expectedOutput
  }

  it should "return the metric name without labels if the labels map is empty" in {
    val name = "my_metric"
    val labels = Map.empty[String, String]
    val expectedOutput = "my_metric"
    val actualOutput = StreamingQuerySource.metricWithLabels(name, labels)
    actualOutput shouldEqual expectedOutput
  }

  "extractSourceName" should "return the Kafkfa source name from a string" in {
    val input = "KafkaV2[Subscribe[mpathic-event]]"
    val expectedOutput = "KafkaV2"
    val actualOutput = StreamingQuerySource.extractSourceName(input)
    actualOutput shouldEqual expectedOutput
  }

  "extractSourceName" should "return the Iceberg source name from a string" in {
    val input = "org.apache.iceberg.spark.source.SparkMicroBatchStream@32c41681"
    val expectedOutput = "IcebergMicroBatchStream"
    val actualOutput = StreamingQuerySource.extractSourceName(input)
    actualOutput shouldEqual expectedOutput
  }

  "extractSubscriptionName" should "return the kafka topic from the description" in {
    val input = "KafkaV2[Subscribe[mpathic-event]]"
    val expectedOutput = "mpathic-event"
    val actualOutput = StreamingQuerySource.extractSubscriptionName(input)
    actualOutput shouldEqual expectedOutput
  }

  "getOffset" should "parse a valid offset JSON string into a map of topics to partitions to offsets" in {
    val input = """{
      "mpathic-event": {
          "0": 689125037,
          "1": 597416917,
          "2": 589446933,
          "3": 444659423,
          "4": 432998635,
          "5": 429711474,
          "6": 445322191,
          "7": 432412809,
          "8": 431092969,
          "9": 444619591
      },
      "topic2": {
          "0": 689125037,
          "1": 597416917,
          "2": 589446933,
          "3": 444659423,
          "4": 432998635
      }
    }"""
    val expectedOutput = Map(
      "mpathic-event" -> Map(
        "0" -> 689125037,
        "1" -> 597416917,
        "2" -> 589446933,
        "3" -> 444659423,
        "4" -> 432998635,
        "5" -> 429711474,
        "6" -> 445322191,
        "7" -> 432412809,
        "8" -> 431092969,
        "9" -> 444619591
      ),
      "topic2" -> Map(
        "0" -> 689125037,
        "1" -> 597416917,
        "2" -> 589446933,
        "3" -> 444659423,
        "4" -> 432998635
      )
    )
    val actualOutput = StreamingQuerySource.getOffset(input)
    actualOutput shouldEqual expectedOutput
  }

  it should "parse a valid empty JSON into an empty map" in {
    val input = "{}"
    val expectedOutput = Map.empty[String, Map[String, Long]]
    val actualOutput = StreamingQuerySource.getOffset(input)
    actualOutput shouldEqual expectedOutput
  }
  it should "return an empty map if the string is empty" in {
    val input = ""
    val expectedOutput = Map.empty[String, Map[String, Long]]
    val actualOutput = StreamingQuerySource.getOffset(input)
    actualOutput shouldEqual expectedOutput
  }

  it should "return an empty map if the string is not a json object" in {
    val input = """["some data"]"""
    val expectedOutput = Map.empty[String, Map[String, Long]]
    val actualOutput = StreamingQuerySource.getOffset(input)
    actualOutput shouldEqual expectedOutput
  }
}
