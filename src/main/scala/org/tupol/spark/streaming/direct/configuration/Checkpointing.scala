package org.tupol.spark.streaming.direct.configuration

import org.tupol.utils.config.Configurator

/**
 * Marker trait that indicates that the implementing class is aware
 * of the Spark checkpointing configuration.
 */
trait SparkCheckpointing {
  def checkpointing: Checkpointing
}

/**
 * Configuration class for the checkpointing feature
 *
 * @param directory checkpoint directory path
 */
case class Checkpointing(directory: String)

object Checkpointing extends Configurator[Checkpointing] {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, Checkpointing] =
    config.extract[String]("checkpointing.directory").map(Checkpointing(_))

}
