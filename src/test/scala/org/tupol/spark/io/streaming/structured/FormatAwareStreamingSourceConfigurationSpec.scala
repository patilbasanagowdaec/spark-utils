package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.sources.{ JsonSourceConfiguration, TextSourceConfiguration }
import org.tupol.spark.sql.loadSchemaFromFile
import org.tupol.utils.config._

class FormatAwareStreamingSourceConfigurationSpec extends FunSuite with Matchers with SharedSparkSession {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")

  test("Successfully extract text FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract json FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="json"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = JsonSourceConfiguration())
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract kafka FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafka.bootstrap.servers="my_server"
        |input.subscription.type="subscribePattern"
        |input.subscription.value="topic_*"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "my_server",
      subscription = KafkaSubscription("subscribePattern", "topic_*"))
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract generic FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format=kafka
        |input.options {}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSourceConfiguration(Kafka, Map())
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

}
