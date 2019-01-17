package org.tupol.spark.streaming.configuration

import org.apache.spark.streaming.kafka010.{ConsumerStrategy, LocationStrategy}
import org.tupol.utils.config.Configurator

/**
 * Kafka connection parameters
 */
case class KafkaConnectionParameters(kafkaBrokers: String, kafkaTopics: Seq[String],
                                     kafkaKeySerializer: String, kafkaValueSerializer: String,
                                     locationStrategy: LocationStrategy, consumerStrategy: ConsumerStrategy)

object KafkaConnectionParameters extends Configurator[KafkaConnectionParameters] {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, KafkaConnectionParameters] = {
    import scalaz.syntax.applicative._
    import scalaz.syntax.validation._
    config.extract[String]("kafka.brokers") |@|
      config.extract[String]("kafka.topics").map(_.split(",").map(_.trim).toSeq) |@|
      config.extract[Option[String]]("kafka.key.serializer").map(_.getOrElse("org.apache.kafka.common.serialization.StringSerializer")) |@|
      config.extract[Option[String]]("kafka.value.serializer").map(_.getOrElse("org.apache.kafka.common.serialization.StringSerializer")) |@|
      config.extract[Boolean]("kafka.strategy.location") |@|
      config.extract[Option[String]]("kafka.strategy.consumer").orElse(Some("largest").success) apply
      KafkaConnectionParameters.apply
  }
}

/**
 * Settings related to storing kafka streaming offsets in zookeeper.
 */
case class ZookeeperOffsetsConfiguration(zkHosts: String, zkPath: String, zkSessionTimeout: Int, zkConnectionTimeout: Int)

object ZookeeperOffsetsConfiguration extends Configurator[ZookeeperOffsetsConfiguration] {
  import com.typesafe.config.Config
  import scalaz.ValidationNel
  def validationNel(config: Config): ValidationNel[Throwable, ZookeeperOffsetsConfiguration] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._
    config.extract[String]("kafka.zk.offsets.zkHosts") |@|
      config.extract[String]("kafka.zk.offsets.zkPath") |@|
      config.extract[Int]("kafka.zk.offsets.zkSessionTimeout") |@|
      config.extract[Int]("kafka.zk.offsets.zkConnectionTimeout") apply ZookeeperOffsetsConfiguration.apply
  }
}

/**
 * Marker class that indicates the implementor is aware of kafka connection parameters.
 */
sealed trait KafkaInputStreamSourceConfiguration extends StreamSourceConfiguration {
  def connectionParameters: KafkaConnectionParameters
}
