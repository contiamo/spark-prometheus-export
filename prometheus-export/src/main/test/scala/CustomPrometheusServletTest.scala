import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import CustomPrometheusServlet.parseMetricKey

class ParseMetricKeySpec extends AnyFlatSpec with Matchers {
  it should "parse a metric key with no labels" in {
    val input = "response.count"
    parseMetricKey(input) shouldEqual (input, Map.empty)
  }

  it should "parse a metric key with three labels" in {
    val input =
      "response.http.count.LABELS.user_agent.Mozilla.version.1.status.ok"
    parseMetricKey(input) shouldEqual (
      "response.http.count",
      Map("user_agent" -> "Mozilla", "version" -> "1", "status" -> "ok")
    )
  }

  it should "parse a metric key with one label" in {
    val input = "response.grpc.count.LABELS.user_agent.Mozilla"
    parseMetricKey(input) shouldEqual (
      "response.grpc.count",
      Map("user_agent" -> "Mozilla")
    )
  }

  it should "ignore empty label values" in {
    val input = "response.count.LABELS.user_agent.Mozilla.value."
    parseMetricKey(input) shouldEqual (
      "response.count",
      Map("user_agent" -> "Mozilla")
    )
  }
}
