package org.tupol.spark.streaming.direct

import org.apache.spark.streaming.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.streaming.StringSocketSpec
import org.tupol.spark.streaming.direct.configuration.SocketStreamSourceConfiguration
import org.tupol.spark.streaming.mocks._

import scala.util.{ Failure, Success }

class TextStringStreamFactoryTest extends FlatSpec
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SparkStreamingSpec with StringSocketSpec
  with TextSocketInputStreamFactory[SocketStreamSourceConfiguration] {

  val TestConfig = SocketStreamSourceConfiguration("localhost", port)

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "String messages" should "be written to the socket stream, transformed and read back" in {

    val messages = scala.collection.mutable.ArrayBuffer.empty[String]

    createStream(ssc, TestConfig) match {
      case Success(stream) => stream.foreachRDD(rdd => messages ++= rdd.map(_._2).map(dummyFun).collect())
      case Failure(e) => logError("Failed to create stream!", e)
    }

    // Start receiving data
    ssc.start()

    // First data in kafka, should be saved and offset 0 (fromOffset) saved in ZK
    send("test-message1")
    advanceClock(batchDuration)
    eventually {
      messages(0) shouldEqual dummyFun("test-message1")
    }
    // Second data in kafka, should be saved and offset 1 (fromOffset)  saved in ZK
    send("test-message2")
    advanceClock(batchDuration)
    eventually {
      messages(1) shouldEqual dummyFun("test-message2")
    }
    messages.size shouldBe 2
    // We end the first test here, which simulates the job going down
  }

}
