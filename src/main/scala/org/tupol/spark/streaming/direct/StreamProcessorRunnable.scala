package org.tupol.spark.streaming.direct

import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.tupol.spark.SparkApp
import org.tupol.spark.streaming.direct.configuration.{ SparkCheckpointing, SparkStreamingInterval, StreamSourceConfiguration }
import org.tupol.utils._

import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

/**
 * Simple trait for basic streaming processing providing also the infrastructure to make it runnable.
 *
 * For a simple example on how to use this API to create a Kafka consumer with both before and after offsets
 * that simply writes to an HDFS Json file, see src/test/examples/DemoStreamProcessorRunnable.scala.
 *
 * @tparam K type of Stream message key
 * @tparam V type of Stream message value
 * @tparam KD type of Stream message key decoder
 * @tparam VD type of Stream message value decoder
 * @tparam C type of configuration case class being passed in.
 */
abstract class StreamProcessorRunnable[K: ClassTag, V: ClassTag, C <: SparkStreamingInterval with StreamSourceConfiguration]
  extends SparkApp[C, Unit] with StreamProcessor[K, V, C] with InputStreamFactory[K, V, C] {

  /**
   * Factory for processingFunction.
   * Sometimes a processing function needs to be created at runtime, as it depends on external configuration.
   *
   * @param ssc
   * @param config
   * @return the processing function that will be used to consume the stream
   */
  def createProcessingFunction(ssc: StreamingContext, config: C): Try[ProcessingFunction]

  /**
   * Create a StreamingContext taking into account checkpointing if configured.
   * @param sc
   * @param config
   * @return
   */
  def createStreamingContext(sc: SparkContext, config: C): Try[StreamingContext] = Try {

    def createBasicStreamingContext: StreamingContext = StreamingContext.getActiveOrCreate(() => {
      val ssc = new StreamingContext(sc, config.interval.batchDuration)
      // This ".get" is on purpose; by the way the setupStream() function is written, it returns a Try[DStream],
      // but here we need to return the StreamingContext, so we force a get that might throw an exception, that will
      // be caught by the surrounding Try
      setupStream(ssc, config).get
      ssc
    })

    config match {
      case checkpoint: SparkCheckpointing =>
        logInfo(s"Creating a new StreamingContext with checkpointing at '${checkpoint.checkpointing.directory}'.")
        def createStreamingContext = {
          val ssc = createBasicStreamingContext
          ssc.checkpoint(checkpoint.checkpointing.directory)
          ssc
        }
        StreamingContext.getActiveOrCreate(checkpoint.checkpointing.directory, createStreamingContext _)
      case _ =>
        logInfo("Creating a new StreamingContext without checkpointing.")
        createBasicStreamingContext
    }

  }

  /**
   * Setup a stream by creating the processing function, create the input stream and apply the processing function
   * @param ssc
   * @param config
   * @return
   */
  protected def setupStream(ssc: StreamingContext, config: C): Try[DStream[(K, V)]] =
    for {
      _ <- Success(logInfo("Create the processing function."))
      processingFunction <- createProcessingFunction(ssc, config)
        .logFailure(logError("Failed to create the processing function.", _))

      _ <- Success(logInfo("Create the input stream."))
      stream <- createStream(ssc, config)
        .logFailure(logError(s"Not possible to create a stream in $appName.", _))
        .recoverWith {
          case t =>
            sys.ShutdownHookThread { closeStreamingContext(ssc) }
            Failure(t)
        }

      _ <- Success(logInfo("Setup the stream processing."))
      _ <- process(stream, config, processingFunction)
        .logFailure(logError(s"Not possible to create a stream in $appName.", _))
    } yield stream

  /**
   * Gracefully close the streaming context.
   * @param ssc
   */
  private def closeStreamingContext(ssc: StreamingContext) = {
    logInfo(s"Gracefully stopping spark streaming $appName application...")
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    logInfo(s"Spark streaming $appName application stopped.")
  }

  /**
   * Basic runJob implementation: create the stream, get the processing function and process the stream.
   *
   * @param sc
   * @param config
   */
  final def run(sc: SparkContext, config: C): Try[Unit] = {

    for {
      ssc <- createStreamingContext(sc, config)

      _ <- Success(logInfo("Starting the stream processing."))
      streamProcess <- Try(ssc.start())
        .map(_ => ssc.awaitTermination())
        .logFailure(logError(
          """Error while running the stream.
            |If the exception bellow provides no clue, please double check the entire configuration and pay special attention
            |to the stream configuration (start with the topics).""".stripMargin, _))
        .recoverWith {
          case t: Throwable =>
            closeStreamingContext(ssc)
            Failure(t)
        }
    } yield streamProcess

  }

}
