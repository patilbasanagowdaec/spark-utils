package org.tupol.spark.streaming.direct

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.tupol.spark.streaming.direct.configuration._
import org.tupol.spark.streaming.direct.configuration.kafka_010.KafkaConnectionConfiguration

import scala.util.Try

/**
 * Common trait for Stream factories
 *
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 * @tparam KD type of Kafka message key decoder
 * @tparam VD type of Kafka message value decoder
 */
trait InputStreamFactory[K, V, C <: StreamSourceConfiguration] extends Serializable with Logging {
  def createStream(ssc: StreamingContext, config: C): Try[DStream[(K, V)]]
}

/**
 * Text file stream factory
 */
trait TextFileInputStreamFactory[C <: FileStreamSourceConfiguration] extends InputStreamFactory[String, String, C] {
  def createStream(ssc: StreamingContext, config: C): Try[DStream[(String, String)]] = Try {
    ssc.textFileStream(config.inputFileStream).map((null, _))
  }
}

/**
 * Socket stream factory for text input
 */
trait TextSocketInputStreamFactory[C <: SocketStreamSourceConfiguration] extends InputStreamFactory[String, String, C] {
  def createStream(ssc: StreamingContext, config: C): Try[DStream[(String, String)]] = Try {
    // Making the key null at the end is not a very good option
    ssc.socketTextStream(config.host, config.port).map((null, _))
  }
}

/**
 * Simple Kafka stream factory, no offset saving
 *
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
trait KafkaSimpleInputStreamFactory[K, V, C <: KafkaConnectionConfiguration]
  extends InputStreamFactory[K, V, C] {

  /**
   * Creates a regular Kafka stream with the option of keeping offsets in Zookeeper.
   * This approach implements the at least once semantics, since it saves the offsets of the previous batch (fromOffset) in ZK.
   * The reason for that is because it is better to process the same event twice in case of a failure
   * than not to process it at all.
   */
  def createStream(ssc: StreamingContext, config: C): Try[DStream[(K, V)]] =
    config.consumerStrategy[K, V].flatMap {
      case consumerStrategy =>
        Try(KafkaUtils.createDirectStream[K, V](ssc, config.locationStrategy, consumerStrategy)
          .map(cr => (cr.key(), cr.value())))
    }
}
