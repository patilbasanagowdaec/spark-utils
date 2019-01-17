package org.tupol.spark

import org.apache.spark.streaming.dstream.DStream
import org.tupol.spark.streaming.configuration.StreamingInterval

import scala.reflect.ClassTag

package object streaming {

  /**
   * Window a stream based on the `StreamingInterval` configuration
   */
  def windowStream[T](stream: DStream[T], interval: StreamingInterval) =
    (interval.windowDuration, interval.slideDuration) match {
      case (Some(windowDuration), Some(slideDuration)) => stream.window(windowDuration, slideDuration)
      case (Some(windowDuration), None) => stream.window(windowDuration)
      case _ => stream
    }

  /**
   * Reduce a keyed stream by key and window or just by key if no windowing information is available
   * @param stream
   * @param reduceFunction
   * @param invReduceFunction
   * @param interval
   * @tparam K
   * @tparam V
   * @return
   */
  def reduceByKeyAndWindow[K: ClassTag, V: ClassTag](
    stream: DStream[(K, V)],
    reduceFunction: (V, V) => V,
    invReduceFunction: Option[(V, V) => V],
    interval: StreamingInterval
  ): DStream[(K, V)] =
    (interval.windowDuration, interval.slideDuration, invReduceFunction) match {
      case (Some(windowDuration), Some(slideDuration), Some(invRedFun)) =>
        stream.reduceByKeyAndWindow(reduceFunction, invRedFun, windowDuration, slideDuration)
      case (Some(windowDuration), None, Some(invRedFun)) =>
        stream.reduceByKeyAndWindow(reduceFunction, invRedFun, windowDuration)
      case _ => stream.reduceByKey(reduceFunction)
    }

}
