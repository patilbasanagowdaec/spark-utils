package org.tupol.spark.streaming.direct

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.tupol.spark.streaming.direct.configuration.StreamSourceConfiguration

import scala.reflect.ClassTag
import scala.util.Try

/**
 * This trait defines the basic processing for a stream. In most of the cases this trait should not be used directly, but rather through its subclasses.
 *
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 * @tparam KD type of Kafka message key decoder
 * @tparam VD type of Kafka message value decoder
 * @tparam C type of configuration class
 * @see nl.ing.mlx.utils.streaming.StreamProcessorRunnable
 */
trait StreamProcessor[K, V, C <: StreamSourceConfiguration] {

  type ProcessingFunction = (C, RDD[(K, V)]) => Unit

  /**
   * This is the workhorse of the StreamProcessor.
   * For now the main use case is consuming a DStream of strings.
   *
   * @param config
   * @param processingFunction this is the function that processes the stream
   */
  protected def process(stream: DStream[(K, V)], config: C, processingFunction: ProcessingFunction)(implicit ktag: ClassTag[K], vtag: ClassTag[V]): Try[Unit] =
    Try {
      stream.foreachRDD { rdd =>
        processingFunction(config, rdd)
      }
    }

}
