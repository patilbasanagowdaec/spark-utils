package org.tupol.spark.streaming.configuration

import java.util

import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{ConsumerStrategy, _}
import org.tupol.utils._
import org.tupol.utils.config._

import scala.collection.JavaConverters._

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
          s"Supported strategies are: ${AcceptableStrategyNames.mkString("'", "', '", "'")}").toNel
      )(name => AcceptableStrategyNames.contains(name)).get
      val hostMap = config.extract[Option[Map[String, TopicPartition]]].
        map(_.getOrElse(Map[String, TopicPartition]())).get.map(_.swap)
      name.trim.toLowerCase match {
        case "preferbrokers" => LocationStrategies.PreferBrokers
        case "preferconsistent" => LocationStrategies.PreferConsistent
        case "preferfixed" => LocationStrategies.PreferFixed(hostMap)
      }
    }
  }

//  case class ConsumerStrategyConfiguration[K, V](name: String, kafkaParams: Map[String, Any], pattern: Option[String],
//                                                 topicPartitions: Option[Seq[TopicPartition]],
//                                                 offsets: Option[Map[TopicPartition, Long]])
  implicit def ConsumerStrategyConfigurationExtractor[K, V] = new Extractor[ConsumerStrategy[K, V]] {
    val AcceptableStrategyNames = Set("Subscribe", "SubscribePattern", "Assign")
    def extract(config: Config, path: String): ConsumerStrategy[K, V] = {
      val name = config.extract[String]("name").ensure(
        new IllegalArgumentException(s"The provided strategy name is not supported. " +
          s"Supported strategies are: ${AcceptableStrategyNames.mkString("'", "', '", "'")}").toNel
      )(name => AcceptableStrategyNames.contains(name)).get
      val pattern = config.extract[Option[String]]("pattern").get
      val topics: Option[Seq[String]] = config.extract[Option[Seq[String]]]("topics").get
      val kafkaParams = config.extract[Map[String, Any]]("kafka_params").get
      val offsets = config.extract[Option[Map[String, TopicPartition]]]("offsets").
        map(_.map(_.map(x => (x._2, x._1.toLong)))).get
      val topicPartitions: Option[Seq[TopicPartition]] = config.extract[Option[Seq[TopicPartition]]]("topic_partitions").get
      name.trim.toLowerCase match {
        case "subscribe" =>
          (topics, offsets) match {
            case (Some(tx), Some(ox)) =>
              ConsumerStrategies.Subscribe[K, V](
                tx.asJava.asInstanceOf[util.Collection[java.lang.String]],
                kafkaParams.mapValues(_.asInstanceOf[Object]).asHashMap,
                ox.mapValues(_.asInstanceOf[java.lang.Long]).asHashMap)
            case (Some(tx), None) =>
              ConsumerStrategies.Subscribe[K, V](
                tx.asJava.asInstanceOf[util.Collection[java.lang.String]],
                kafkaParams.mapValues(_.asInstanceOf[Object]).asHashMap)
          }
      }
    }
  }


  /**
   * Kafka connection parameters
   */
  case class KafkaConnectionParameters[K, V](kafkaBrokers: String, kafkaTopics: Seq[String],
                                       locationStrategy: LocationStrategy, consumerStrategy: ConsumerStrategy[K, V])
    extends Configurator[KafkaConnectionParameters[K, V]] {
    import com.typesafe.config.Config
    import org.tupol.utils.config._
    import scalaz.ValidationNel

    def validationNel(config: Config): ValidationNel[Throwable, KafkaConnectionParameters[K, V]] = {
      import scalaz.syntax.applicative._
      config.extract[String]("kafka.brokers") |@|
        config.extract[String]("kafka.topics").map(_.split(",").map(_.trim).toSeq) |@|
        config.extract[LocationStrategy]("kafka.location_strategy") |@|
        config.extract[ConsumerStrategy[K, V]]("kafka.consumer_strategy") apply
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
  sealed trait KafkaInputStreamSourceConfiguration[K, V] extends StreamSourceConfiguration {
    def connectionParameters: KafkaConnectionParameters[K, V]
  }

}
