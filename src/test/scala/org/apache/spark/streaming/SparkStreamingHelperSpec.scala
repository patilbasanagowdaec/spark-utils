package org.apache.spark.streaming

import org.apache.spark.Logging
import org.apache.spark.util.ManualClock

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.{ Duration => SDuration, MILLISECONDS }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
 * This is a basic helper trait to be shared between the two major users:
 * `SparkStreamingSpec` and `SparkStreamingCheckpointingSpec`.
 *
 * Based on: [[http://mkuthan.github.io/blog/2015/03/01/spark-unit-testing/]]
 *
 * See also: [[https://github.com/mkuthan/example-spark]]
 */
private[streaming] trait SparkStreamingHelperSpec extends Logging {

  def batchDuration: Duration = Seconds(3)

  def timeoutDuration: Duration = batchDuration

  /**
   * Provide an external handle to the streaming context
   * @return
   */
  def ssc: StreamingContext

  private def manualClock: ManualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]

  def advanceClock(timeToAdd: Duration): Unit = manualClock.advance(timeToAdd.milliseconds)

  def advanceClockOneBatch: Unit = manualClock.advance(batchDuration.milliseconds)

  /**
   * Main tool for testing stream runnables.
   * Main steps:
   * 1. Start the runnable in a new thread
   * 2. Start a timeout clock thread that will stop the streaming context in case it goes haywire
   * 3. Wait for an active streaming context (basically whenever the ssc.start is called;
   *    normally this will happen inside the runnable
   *
   * @param runnable
   * @tparam T
   * @return
   */
  def testRunnable[T](runnable: => T): Unit = {
    // The runnable is blocking so we "unblock" it
    logDebug(s"Starting the runnable...")
    Future(runnable)
    // It will take a bit to initialize the context and start the stream so wait for it
    waitForActiveContext

    def waitForActiveContext = {
      logDebug(s"Waiting for an active StreamingContext...")
      while (StreamingContext.getActive().isEmpty) Thread.sleep(10)
      logDebug(s"Active StreamingContext available.")
    }
  }

  private[streaming] def closeStreamingContext(ssc: StreamingContext): Unit = {
    if (ssc != null && ssc.getState != StreamingContextState.STOPPED) {
      logDebug(s"Stopping StreamingContext...")
      ssc.stop(false, false)
      logDebug(s"StreamingContext was stopped.")
    } else {
      logDebug(s"No StreamingContext was found or it was already stopped. No harm, no foul.")
    }
  }

  private[streaming] def runAfterTimeout[T](timeout: Duration)(block: => T) = {
    logDebug(s"Starting the stop timer. It will be stopped in about $timeout.")
    Try {
      Await.result(Future[Unit] {
        Thread.sleep(timeout.milliseconds)
      }, SDuration(timeout.milliseconds, MILLISECONDS))
    }
    logDebug(s"Running code after $timeout.")
    val result = block
    logDebug(s"The code finished running after $timeout.")
    result
  }

  private[streaming] def sscTimeoutClose(ssc: StreamingContext) = runAfterTimeout(timeoutDuration) { closeStreamingContext(ssc) }

}
