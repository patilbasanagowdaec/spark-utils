package org.tupol.spark.streaming.configuration

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.{FunSuite, Matchers}

class StreamingIntervalSpec extends FunSuite with Matchers {

  test("from batch in millis and window in seconds should work") {
    val batchTimeValue = 7
    val windowTimeValue = 21
    val config = ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(s"$batchTimeValue"))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(s"$windowTimeValue"))
    val result = StreamingInterval(config).get
    result.batchDuration.milliseconds shouldBe batchTimeValue
    result.windowDuration.get.milliseconds shouldBe windowTimeValue * 1000
  }

  test("from batch in seconds and window in minutes should work") {
    val batchTimeValue = 7
    val windowTimeValue = 21
    val config = ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(s"$batchTimeValue"))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(s"$windowTimeValue"))
    val result = StreamingInterval(config).get
    result.batchDuration.milliseconds shouldBe batchTimeValue * 1000
    result.windowDuration.get.milliseconds shouldBe windowTimeValue * 60 * 1000
  }

  test("from batch in minutes and window in minutes should work") {
    val batchTimeValue = 7
    val windowTimeValue = 21
    val config = ConfigFactory.empty
      .withValue("batch.duration.minutes", ConfigValueFactory.fromAnyRef(s"$batchTimeValue"))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(s"$windowTimeValue"))
    val result = StreamingInterval(config).get
    result.batchDuration.milliseconds shouldBe batchTimeValue * 60 * 1000
    result.windowDuration.get.milliseconds shouldBe windowTimeValue * 60 * 1000
  }

  test("from batch in millis and window in millis and slide in millis should work ") {
    val batchTimeValue = 7
    val windowTimeValue = 21
    val slideTimeValue = 14
    val config = ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(s"$batchTimeValue"))
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(s"$windowTimeValue"))
      .withValue("slide.duration.milliseconds", ConfigValueFactory.fromAnyRef(s"$slideTimeValue"))
    val result = StreamingInterval(config).get
    result.batchDuration.milliseconds shouldBe batchTimeValue
    result.windowDuration.get.milliseconds shouldBe windowTimeValue
    result.slideDuration.get.milliseconds shouldBe slideTimeValue
  }

  test("from batch in millis and no window should work") {
    val batchTimeValue = 7
    val config = ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(s"$batchTimeValue"))
    val result = StreamingInterval(config).get
    result.batchDuration.milliseconds shouldBe batchTimeValue
    result.windowDuration shouldBe None
  }

}
