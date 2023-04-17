import java.util.Properties
import com.codahale.metrics.{
  Counter,
  Gauge,
  Histogram,
  Meter,
  Timer,
  MetricRegistry
}
import javax.servlet.http.HttpServletRequest
import org.apache.spark.metrics.sink.CustomPrometheusServlet
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CustomPrometheusServletSpec extends AnyFlatSpec with Matchers {

  val testProperties: Properties = {
    val properties = new Properties()
    properties
  }

  "getMetricsSnapshot" should "return a correct string representation for each metric type" in {
    // Prepare the registry with different metric types
    val registry = new MetricRegistry()

    // Counter metric
    val counter = registry.counter(
      "counter.metric.LABELS.label_key1.label_val1.label_key2.label_val2"
    )
    counter.inc(5)

    // Gauge metric
    val gauge = registry.register(
      "gauge.metric.LABELS.label_key1.label_val1.label_key2.label_val2",
      new Gauge[Double] {
        override def getValue: Double = 42.0
      }
    )

    // Histogram metric
    val histogram = registry.histogram(
      "histogram.metric.LABELS.label_key1.label_val1.label_key2.label_val2"
    )
    histogram.update(123)
    histogram.update(456)
    histogram.update(789)

    // Meter metric
    val meter = registry.meter(
      "meter.metric.LABELS.label_key1.label_val1.label_key2.label_val2"
    )
    meter.mark(50)
    Thread.sleep(10)
    meter.mark(60)

    // Timer metric
    val timer = registry.timer(
      "timer.metric.LABELS.label_key1.label_val1.label_key2.label_val2"
    )
    val timerContext = timer.time()
    Thread.sleep(10)
    timerContext.stop()

    timer.time()
    Thread.sleep(5)
    timerContext.stop()

    timer.time()
    Thread.sleep(6)
    timerContext.stop()

    val servlet = new CustomPrometheusServlet(testProperties, registry)
    val exampleRequest: HttpServletRequest =
      null // You can pass null since the method does not use the request parameter

    val snap = timer.getSnapshot()
    val timerSum = snap.getValues().sum
    val timer50th = snap.getValue(0.5)
    val timer75th = snap.getValue(0.75)

    // Form the expected output for each metric type
    val expectedOutput =
      s"""
        |# TYPE metrics_counter_metric counter
        |metrics_counter_metric_total{label_key1="label_val1",label_key2="label_val2"} 5
        |
        |# TYPE metrics_gauge_metric gauge
        |metrics_gauge_metric{label_key1="label_val1",label_key2="label_val2"} 42.0
        |
        |# TYPE metrics_histogram_metric summary
        |metrics_histogram_metric_count{label_key1="label_val1",label_key2="label_val2"} 3
        |metrics_histogram_metric_sum{label_key1="label_val1",label_key2="label_val2"} 1368
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.5"} 456.0
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.75"} 789.0
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.95"} 789.0
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.98"} 789.0
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.99"} 789.0
        |metrics_histogram_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.999"} 789.0
        |
        |metrics_meter_metric_count{label_key1="label_val1",label_key2="label_val2"} 110
        |metrics_meter_metric_rate{label_key1="label_val1",label_key2="label_val2",rate="1m"} 0.0
        |metrics_meter_metric_rate{label_key1="label_val1",label_key2="label_val2",rate="5m"} 0.0
        |metrics_meter_metric_rate{label_key1="label_val1",label_key2="label_val2",rate="15m"} 0.0
        |
        |# TYPE metrics_timer_metric summary
        |metrics_timer_metric_count{label_key1="label_val1",label_key2="label_val2"} 3
        |metrics_timer_metric_sum{label_key1="label_val1",label_key2="label_val2"} $timerSum
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.5"} $timer50th
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.75"} $timer75th
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.95"} $timer75th
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.98"} $timer75th
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.99"} $timer75th
        |metrics_timer_metric{label_key1="label_val1",label_key2="label_val2",quantile="0.999"} $timer75th
      """.stripMargin

    val metricsSnapshot = servlet.getMetricsSnapshot(exampleRequest)

    // print(s"metricsSnapshot:\n$metricsSnapshot\n")
    // print(s"expectedOutput:\n$expectedOutput\n")

    // Test if the output matches the expected output for each metric type, ignoring the leading and trailing whitespaces
    metricsSnapshot.trim shouldEqual expectedOutput.trim
  }
}
