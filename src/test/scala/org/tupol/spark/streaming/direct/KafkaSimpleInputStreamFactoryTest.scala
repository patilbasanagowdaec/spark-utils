package org.tupol.spark.streaming.direct

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import kafka.producer.KeyedMessage
import org.apache.spark.SparkException
import org.apache.spark.streaming.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.tupol.spark.streaming.KafkaSpec
import org.tupol.spark.streaming.direct.configuration.kafka_010.KafkaConnectionConfiguration
import org.tupol.spark.streaming.mocks._

import scala.util.{Failure, Success}

class KafkaSimpleInputStreamFactoryTest extends FlatSpec
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SparkStreamingSpec with KafkaSpec
  with KafkaSimpleInputStreamFactory[String, String, KafkaConnectionConfiguration] {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  def typesafeConfig = ConfigFactory.load("kafka-simple-test")
    .withValue(
      "app.streaming.kafka.brokers",
      ConfigValueFactory.fromAnyRef("localhost:" + kafkaPort)).getConfig("app.streaming")

  "String messages" should "be written to kafka, transformed and read back" in {

    val messages = scala.collection.mutable.ArrayBuffer.empty[String]

    createStream(ssc, KafkaConnectionConfiguration(typesafeConfig).get) match {
      case Success(stream) => stream.foreachRDD(rdd => messages ++= rdd.map(_._2).map(dummyFun).collect())
      case Failure(e) => logError("Failed to create stream!", e)
    }

    // Start receiving data
    ssc.start()

    // First data in kafka, should be saved and offset 0 (fromOffset) saved in ZK
    producer.send(new KeyedMessage[String, String](topicInput, "key", "test-message1"))
    advanceClock(batchDuration)
    eventually {
      messages(0) shouldEqual dummyFun("test-message1")
    }
    // Second data in kafka, should be saved and offset 1 (fromOffset)  saved in ZK
    producer.send(new KeyedMessage[String, String](topicInput, "key", "test-message2"))
    advanceClock(batchDuration)
    eventually {
      messages(1) shouldEqual dummyFun("test-message2")
    }
    messages.size shouldBe 2
    // We end the first test here, which simulates the job going down
  }

  "Failure for configuration" should "be made clear for wrong topics" in {
    val wrongConfig = KafkaConnectionConfiguration(typesafeConfig
      .withValue(
        "stream.kafka.topics",
        ConfigValueFactory.fromAnyRef("THIS_TOPIC_DOES_NOT_EXIST"))).get

    a[SparkException] should be thrownBy createStream(ssc, wrongConfig).get
  }

  it should "be made clear for wrong brokers" in {

    val wrongConfig = KafkaConnectionConfiguration(typesafeConfig
      .withValue(
        "stream.kafka.brokers",
        ConfigValueFactory.fromAnyRef("unknown.host:000000"))).get

    a[SparkException] should be thrownBy createStream(ssc, wrongConfig).get
  }

}
