package org.apache.spark.streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.tupol.spark.SharedSparkSession

/**
 * Basic test framework for Spark Streaming applications and StreamProcessorRunnable applications that use checkpointing.
 */
trait SparkStreamingCheckpointingSpec extends SharedSparkSession with BeforeAndAfterEach with SparkStreamingHelperSpec {
  this: Suite =>

  def checkpointDirectory: File = new File(
    FileUtils.getTempDirectoryPath,
    s"mlx-checkpointing-test-${getClass.getSimpleName.replaceAll("\\$", "")}"
  )

  @transient private var _ssc: StreamingContext = _

  private def createStreamingContext = {
    logInfo(s"Creating a new StreamingContext with checkpointing at '$checkpointDirectory'.")
    val ssc = new StreamingContext(sc, batchDuration)
    ssc.checkpoint(checkpointDirectory.getAbsolutePath)
    ssc
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Let's make sure that indeed there is nothing in here and recreate the checkpointing directory
    if (checkpointDirectory.exists) {
      FileUtils.forceDelete(checkpointDirectory)
      logDebug(s"Deleted checkpoint directory at $checkpointDirectory")
    }
    FileUtils.forceMkdir(checkpointDirectory)
  }

  override def afterEach(): Unit = {
    // We need to stop the stream, but let it run for a bit
    sscTimeoutClose(ssc)
    if (checkpointDirectory.exists) {
      FileUtils.forceDelete(checkpointDirectory)
      logDebug(s"Deleted checkpoint directory at $checkpointDirectory")
    }
    super.afterEach()
  }

  def ssc: StreamingContext = {
    val activeSsc = StreamingContext.getActiveOrCreate(checkpointDirectory.getAbsolutePath, () => createStreamingContext)
    if (activeSsc != _ssc && _ssc != null) {
      logDebug("The active streaming context is different from the underlying streaming context. The underlying will be stopped.")
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
