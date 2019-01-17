package org.tupol.spark.streaming.configuration

import scalaz.ValidationNel

/**
 * SocketStream specific configuration parameters.
 */
case class SocketStreamSourceConfiguration(host: String, port: Int) extends StreamSourceConfiguration

object SocketStreamSourceConfiguration {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.syntax.applicative._

  def validationNel(config: Config): ValidationNel[Throwable, SocketStreamSourceConfiguration] = {
    config.extract[String]("socket.host") |@|
      config.extract[Int]("socket.port") apply
      SocketStreamSourceConfiguration.apply
  }
}

