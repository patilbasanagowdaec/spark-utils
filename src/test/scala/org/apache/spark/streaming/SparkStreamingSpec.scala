package org.apache.spark.streaming

import org.scalatest.{ BeforeAndAfterEach, Suite }
import org.tupol.spark.SharedSparkSession

/**
 * Basic test framework for Spark Streaming applications and StreamProcessorRunnable applications.
 *
 * Based on: [[http://mkuthan.github.io/blog/2015/03/01/spark-unit-testing/]]
 *
 * See also: [[https://github.com/mkuthan/example-spark]]
 */
trait SparkStreamingSpec extends SharedSparkSession with BeforeAndAfterEach with SparkStreamingHelperSpec {
  this: Suite =>

  private var _ssc: StreamingContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    _ssc = new StreamingContext(sc, batchDuration)
  }

  override def afterEach(): Unit = {
    if (_ssc != null) {
      _ssc.stop(false)
      _ssc = null
    }
    super.afterEach()
  }

  def ssc: StreamingContext = {
    val activeSsc = StreamingContext.getActiveOrCreate(() => _ssc)
    if (activeSsc != _ssc && _ssc != null) {
      _ssc.stop(false)
    }
    _ssc = activeSsc
    _ssc
  }

  override def sparkConfig: Map[String, String] = {
    // Switch the SystemClock with a ManualClock for more control
    super.sparkConfig +
      ("spark.streaming.clock" -> "org.apache.spark.streaming.util.ManualClock") +
      ("spark.streaming.stopGracefullyOnShutdown" -> "true")
  }

}
