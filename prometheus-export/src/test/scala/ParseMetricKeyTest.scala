import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.metrics.sink.CustomPrometheusServlet.{parseMetricKey, MetricsKey}

class ParseMetricKeySpec extends AnyFlatSpec with Matchers {

  it should "parse a metric key with no labels" in {
    val input = "response.count"
    val MetricsKey(name, labels) = parseMetricKey(input)
    name shouldEqual "metrics_response_count"
    labels shouldEqual Map.empty
  }

  it should "parse a metric key with three labels" in {
    val input =
      "response.http.count.LABELS.user_agent.Mozilla.version.1.status.ok"
    val MetricsKey(name, labels) = parseMetricKey(input)
    name shouldEqual "metrics_response_http_count"
    labels("user_agent") shouldEqual "Mozilla"
    labels("version") shouldEqual "1"
    labels("status") shouldEqual "ok"
    labels.size shouldEqual 3
  }

  it should "parse a metric key with one label" in {
    val input = "response.grpc.count.LABELS.user_agent.Mozilla"
    val MetricsKey(name, labels) = parseMetricKey(input)
    name shouldEqual "metrics_response_grpc_count"
    labels("user_agent") shouldEqual "Mozilla"
    labels.size shouldEqual 1
  }

  it should "ignore empty label values" in {
    val input = "response.count.LABELS.user_agent.Mozilla.value."
    val MetricsKey(name, labels) = parseMetricKey(input)
    name shouldEqual "metrics_response_count"
    labels("user_agent") shouldEqual "Mozilla"
    labels.size shouldEqual 1
  }
}
