package org.tupol.spark.streaming.configuration

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.prop.Checkers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{FunSuite, Matchers}
import org.tupol.utils.config.ConfigurationException

import scala.util.Success

class StreamingIntervalProps extends FunSuite with Matchers with Checkers {

  test("from a good configuration should work") {
    forAll(goodConfigurations) { (config) =>
      StreamingInterval(config) shouldBe a[Success[_]]
    }
  }

  test("from a bad configuration should throw a ConfigurationException") {
    forAll(badConfigurations) { (config) =>
      a[ConfigurationException] shouldBe thrownBy(StreamingInterval(config).get)
    }
  }

  val goodConfigurations = Table(
    "Good Configurations",
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(21)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(21))
      .withValue("slide.duration.milliseconds", ConfigValueFactory.fromAnyRef(14)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(21)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(7000)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(21))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(14)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.minutes", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("batch.duration.minutes", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(21)),
    ConfigFactory.empty
      .withValue("batch.duration.minutes", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(21))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(14))
  )

  val badConfigurations = Table(
    "Bad Configurations",
    ConfigFactory.empty,
    ConfigFactory.empty
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("slide.duration.milliseconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.milliseconds", ConfigValueFactory.fromAnyRef(8)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(1)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(1))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.milliseconds", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("batch.duration.milliseconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(1))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(7)),
    ConfigFactory.empty
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(8)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.milliseconds", ConfigValueFactory.fromAnyRef(70)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.seconds", ConfigValueFactory.fromAnyRef(1))
      .withValue("slide.duration.seconds", ConfigValueFactory.fromAnyRef(2)),
    ConfigFactory.empty
      .withValue("batch.duration.seconds", ConfigValueFactory.fromAnyRef(7))
      .withValue("window.duration.minutes", ConfigValueFactory.fromAnyRef(1))
      .withValue("slide.duration.minutes", ConfigValueFactory.fromAnyRef(2))
  )

}
