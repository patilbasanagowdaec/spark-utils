package org.tupol.spark.streaming.direct.configuration

/**
 * FileStream specific configuration parameters.
 */
case class FileStreamSourceConfiguration(inputFileStream: String) extends StreamSourceConfiguration

object FileStreamSourceConfiguration {
  import com.typesafe.config.Config
  import org.tupol.utils.config._

  import scala.util.Try

  def apply(config: Config): Try[FileStreamSourceConfiguration] = {
    config.extract[String]("file.directory").map(FileStreamSourceConfiguration.apply)
  }
}
