package org.tupol.spark.streaming

import kafka.serializer.Decoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.tupol.spark.streaming.configuration._
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.reflect.ClassTag
import scala.util.{Failure, Try}


/**
 * Common trait for Stream factories
 *
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 * @tparam KD type of Kafka message key decoder
 * @tparam VD type of Kafka message value decoder
 */
trait InputStreamFactory[K, V, C <: StreamSourceConfiguration] extends Serializable with Logging {
  def createStream(ssc: StreamingContext, config: C)(implicit ktag: ClassTag[K], vtag: ClassTag[V]): Try[DStream[(K, V)]]
}

/**
 * Text file stream factory
 */
trait TextFileInputStreamFactory[C <: FileStreamSourceConfiguration] extends InputStreamFactory[String, String, C] {
  def createStream(ssc: StreamingContext, config: C)(implicit ktag: ClassTag[String], vtag: ClassTag[String]): Try[DStream[(String, String)]] = Try {
    ssc.textFileStream(config.inputFileStream).map((null, _))
  }
}

/**
 * Socket stream factory for text input
 */
trait TextSocketInputStreamFactory[C <: SocketStreamSourceConfiguration] extends InputStreamFactory[String, String, C] {
  def createStream(ssc: StreamingContext, config: C)(implicit ktag: ClassTag[String], vtag: ClassTag[String]): Try[DStream[(String, String)]] = Try {
    // Making the key null at the end is not a very good option
    ssc.socketTextStream(config.host, config.port).map((null, _))
  }
}


object KafkaInputStreamFactory {

  def buildKafkaParams(connectionParameters: KafkaConnectionParameters): Map[String, String] = {

    val connectionProps = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> connectionParameters.kafkaBrokers
      //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> connectionParameters.kafkaValueSerializer,
      //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> connectionParameters.kafkaKeySerializer
    ) ++
      connectionParameters.startReadingAtOffset.map(x => Map("auto.offset.reset" -> x)).getOrElse(Map[String, String]())

    connectionProps
  }

}


/**
 * Simple Kafka stream factory, no offset saving
 *
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 * @tparam KD type of Kafka message key decoder
 * @tparam VD type of Kafka message value decoder
 */
trait KafkaSimpleInputStreamFactory[K, V, KD <: Decoder[K], VD <: Decoder[V], C <: NoOffsetTrackingStreamSourceConfiguration]
  extends InputStreamFactory[K, V, C] {

  import KafkaInputStreamFactory._
  import org.apache.spark.streaming.kafka010._

  def keyDecoder: KD
  def valueDecoder: VD

  /**
   * Creates a regular Kafka stream with the option of keeping offsets in Zookeeper.
   * This approach implements the at least once semantics, since it saves the offsets of the previous batch (fromOffset) in ZK.
   * The reason for that is because it is better to process the same event twice in case of a failure
   * than not to process it at all.
   */
  def createStream(ssc: StreamingContext, config: C)(implicit ktag: ClassTag[K], vtag: ClassTag[V]): Try[DStream[(K, V)]] = {
    implicit val kd = keyDecoder
    implicit val vd = valueDecoder
    val kafkaParams = buildKafkaParams(config.connectionParameters)
    val topics = config.connectionParameters.kafkaTopics.toSet
    KafkaUtils.createDirectStream[K, V](ssc, kafkaParams, topics)
  }
}
