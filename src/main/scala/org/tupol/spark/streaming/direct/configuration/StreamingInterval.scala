package org.tupol.spark.streaming.direct.configuration

import com.typesafe.config.ConfigException.BadValue
import org.apache.spark.streaming.{ Duration, Milliseconds, Minutes, Seconds }
import org.tupol.utils.config.Configurator

/**
 * Marker trait that indicates that an implementing class is aware
 * of the spark streaming batch interval.
 */
trait SparkStreamingInterval {
  def interval: StreamingInterval
}

case class StreamingInterval(batchDuration: Duration, windowDuration: Option[Duration], slideDuration: Option[Duration])

object StreamingInterval extends Configurator[StreamingInterval] {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._
  import scalaz.syntax.validation._

  def validationNel(config: Config): ValidationNel[Throwable, StreamingInterval] = {

    val batchDuration: ValidationNel[Throwable, Duration] =
      config.extract[Long]("batch.duration.milliseconds").map(Milliseconds(_)) orElse
        config.extract[Long]("batch.duration.seconds").map(Seconds(_)) orElse
        config.extract[Long]("batch.duration.minutes").map(Minutes(_))

    val windowBadValue = new BadValue("window.duration", s"window.duration must be a multiple of the stream.batch.duration.")
    val windowDuration: ValidationNel[Throwable, Option[Duration]] =
      (config.extract[Long]("window.duration.milliseconds").map(d => Some(Milliseconds(d))) orElse
        config.extract[Long]("window.duration.seconds").map(d => Some(Seconds(d))) orElse
        config.extract[Long]("window.duration.minutes").map(d => Some(Minutes(d))) orElse
        None.successNel[Throwable])
        .ensure(windowBadValue.toNel) { optionalDuration =>
          checkMultipleDuration(optionalDuration, batchDuration.toOption)
        }

    val slideBadValue = new BadValue("slide.duration", s"slide.duration must be a multiple of the stream.batch.duration ")
    val slideButNoWindowError = new BadValue("slide.duration", s"slide.duration is defined, but the stream.window.duration is not.")
    val slideDuration: ValidationNel[Throwable, Option[Duration]] =
      (config.extract[Long]("slide.duration.milliseconds").map(d => Some(Milliseconds(d))) orElse
        config.extract[Long]("slide.duration.seconds").map(d => Some(Seconds(d))) orElse
        config.extract[Long]("slide.duration.minutes").map(d => Some(Minutes(d))) orElse
        None.successNel[Throwable])
        .ensure(slideBadValue.toNel)(optionalDuration => checkMultipleDuration(optionalDuration, batchDuration.toOption))
        .ensure(slideButNoWindowError.toNel)(optionalDuration => optionalDuration.forall(_ => windowDuration.toOption.flatten.isDefined))

    batchDuration |@| windowDuration |@| slideDuration apply StreamingInterval.apply
  }

  // Utility function to check weather or not the tested duration is a multiple of the reference duration.
  // We accept that if any of the optional durations are not defined we return true.
  private def checkMultipleDuration(testedDuration: Option[Duration], referenceDuration: Option[Duration]): Boolean = {
    val comparisonResult = for {
      td <- testedDuration
      rd <- referenceDuration
    } yield (td.milliseconds % rd.milliseconds == 0)
    comparisonResult.getOrElse(true)
  }
}
