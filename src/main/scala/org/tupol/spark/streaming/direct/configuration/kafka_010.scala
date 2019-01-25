package org.tupol.spark.streaming.direct.configuration

import java.util
import java.util.regex.Pattern

import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import org.tupol.utils._
import org.tupol.utils.config._
import scalaz.ValidationNel

import scala.collection.JavaConverters._
import scala.util.Try

/**
 *
 */
package object kafka_010 {

  implicit val TopicPartitionExtractor = new Extractor[TopicPartition] {
    override def extract(config: Config, path: String): TopicPartition = {
      val topic = config.extract[String](s"$path.topic").get
      val partition = config.extract[Int](s"$path.partition").get
      new TopicPartition(topic, partition)
    }
  }

  implicit val LocationStrategyExtractor = new Extractor[LocationStrategy] {
    val AcceptableStrategyNames = Set("PreferBrokers", "PreferConsistent", "PreferFixed")
    def extract(config: Config, path: String): LocationStrategy = {
      val name = config.extract[String]("name").ensure(
        new IllegalArgumentException(s"The provided strategy name is not supported. " +
          s"Supported strategies are: ${AcceptableStrategyNames.mkString("'", "', '", "'")}").toNel)(name => AcceptableStrategyNames.contains(name)).get
      val hostMap = config.extract[Option[Map[String, TopicPartition]]].
        map(_.getOrElse(Map[String, TopicPartition]())).get.map(_.swap)
      name.trim.toLowerCase match {
        case "preferbrokers" => LocationStrategies.PreferBrokers
        case "preferconsistent" => LocationStrategies.PreferConsistent
        case "preferfixed" => LocationStrategies.PreferFixed(hostMap)
      }
    }
  }

  trait ConsumerStrategyConfiguration {
    def kafkaParams: Map[String, Any]
    def offsets: Map[TopicPartition, Long]
  }
  implicit object ConsumerStrategyConfiguration extends Configurator[ConsumerStrategyConfiguration] {
    val AcceptableStrategyNames = Set("Subscribe", "SubscribePattern", "Assign")
    override def validationNel(config: Config): ValidationNel[Throwable, ConsumerStrategyConfiguration] = {
      val name = config.extract[String]("name").ensure(
        new IllegalArgumentException(s"The provided strategy name is not supported. " +
          s"Supported strategies are: ${AcceptableStrategyNames.mkString("'", "', '", "'")}").toNel)(name => AcceptableStrategyNames.contains(name))
      name.get.trim.toLowerCase.replaceAll("\\s", "") match {
        case "assign" => config.extract[AssignConsumerStrategyConfiguration]
        case "subscribe" => config.extract[SubscriberConsumerStrategyConfiguration]
        case "subscribepattern" =>
          config.extract[SubscriberPatternConsumerStrategyConfiguration]
      }
    }
  }

  case class SubscriberConsumerStrategyConfiguration(topics: Seq[String], kafkaParams: Map[String, Any],
    offsets: Map[TopicPartition, Long]) extends ConsumerStrategyConfiguration
  implicit object SubscriberConsumerStrategyConfiguration extends Configurator[SubscriberConsumerStrategyConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, SubscriberConsumerStrategyConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._
      val topics = config.extract[Option[Seq[String]]]("topics").map(_.getOrElse(Seq[String]()))
      val kafkaParams = config.extract[Map[String, Any]]("kafka_params")
      val offsets = config.extract[Option[Map[String, TopicPartition]]]("offsets")
        .map(_.getOrElse(Map[String, TopicPartition]()).map(x => (x._2, x._1.toLong)))
      topics |@| kafkaParams |@| offsets apply SubscriberConsumerStrategyConfiguration.apply
    }
  }

  case class SubscriberPatternConsumerStrategyConfiguration(pattern: Pattern, kafkaParams: Map[String, Any],
    offsets: Map[TopicPartition, Long]) extends ConsumerStrategyConfiguration
  implicit object SubscriberPatternConsumerStrategyConfiguration extends Configurator[SubscriberPatternConsumerStrategyConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, SubscriberPatternConsumerStrategyConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._
      val pattern = config.extract[String]("pattern").map(java.util.regex.Pattern.compile(_))
      val kafkaParams = config.extract[Map[String, Any]]("kafka_params")
      val offsets = config.extract[Option[Map[String, TopicPartition]]]("offsets")
        .map(_.getOrElse(Map[String, TopicPartition]()).map(x => (x._2, x._1.toLong)))
      pattern |@| kafkaParams |@| offsets apply SubscriberPatternConsumerStrategyConfiguration.apply
    }
  }

  case class AssignConsumerStrategyConfiguration(topicPartitions: Seq[TopicPartition], kafkaParams: Map[String, Any],
    offsets: Map[TopicPartition, Long]) extends ConsumerStrategyConfiguration
  implicit object AssignConsumerStrategyConfiguration extends Configurator[AssignConsumerStrategyConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, AssignConsumerStrategyConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._
      val topicPartitions = config.extract[Option[Seq[TopicPartition]]]("topic_partitions").map(_.getOrElse(Seq[TopicPartition]()))
      val kafkaParams = config.extract[Map[String, Any]]("kafka_params")
      val offsets = config.extract[Option[Map[String, TopicPartition]]]("offsets")
        .map(_.getOrElse(Map[String, TopicPartition]()).map(x => (x._2, x._1.toLong)))
      topicPartitions |@| kafkaParams |@| offsets apply AssignConsumerStrategyConfiguration.apply
    }
  }

  case class KafkaConnectionConfiguration(kafkaBrokers: String, kafkaTopics: Seq[String],
    locationStrategy: LocationStrategy,
    consumerStrategyConfiguration: ConsumerStrategyConfiguration) extends StreamSourceConfiguration {

    private def ConsumerStrategyBuilder[K, V](configuration: ConsumerStrategyConfiguration): Try[ConsumerStrategy[K, V]] = Try {
      val kafkaParams = configuration.kafkaParams.mapValues(_.asInstanceOf[Object]).asHashMap
      val offsets = configuration.offsets.mapValues(_.asInstanceOf[java.lang.Long]).asHashMap
      configuration match {
        case c: AssignConsumerStrategyConfiguration =>
          val topicPartitions = c.topicPartitions.asJava.asInstanceOf[util.Collection[TopicPartition]]
          ConsumerStrategies.Assign[K, V](topicPartitions, kafkaParams, offsets)
        case c: SubscriberConsumerStrategyConfiguration =>
          val topics = c.topics.asJava.asInstanceOf[util.Collection[java.lang.String]]
          ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, offsets)
        case c: SubscriberPatternConsumerStrategyConfiguration =>
          ConsumerStrategies.SubscribePattern[K, V](c.pattern, kafkaParams, offsets)
      }
    }

    def consumerStrategy[K, V]: Try[ConsumerStrategy[K, V]] = ConsumerStrategyBuilder[K, V](consumerStrategyConfiguration)

  }

  implicit object KafkaConnectionConfiguration extends Configurator[KafkaConnectionConfiguration] {
    import com.typesafe.config.Config
    import org.tupol.utils.config._
    import scalaz.ValidationNel

    def validationNel(config: Config): ValidationNel[Throwable, KafkaConnectionConfiguration] = {
      import scalaz.syntax.applicative._
      config.extract[String]("kafka.brokers") |@|
        config.extract[String]("kafka.topics").map(_.split(",").map(_.trim).toSeq) |@|
        config.extract[LocationStrategy]("kafka.location_strategy") |@|
        config.extract[ConsumerStrategyConfiguration]("kafka.consumer_strategy") apply
        KafkaConnectionConfiguration.apply
    }
  }

}
