/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark.io

import org.apache.spark.sql.{ DataFrame, DataFrameWriter, Row }
import org.tupol.spark.Logging
import org.tupol.utils.config.Configurator

import scala.util.{ Failure, Success, Try }

/**  GenericDataSink trait */
case class GenericDataSink(configuration: GenericSinkConfiguration) extends DataSink[GenericSinkConfiguration, DataFrame] with Logging {

  /** Configure a `writer` for the given `DataFrame` based on the given `JdbcDataSinkConfig` */
  private def configureWriter(data: DataFrame, configuration: GenericSinkConfiguration): DataFrameWriter[Row] = {
    data.write
      .format(configuration.format.toString)
      .mode(configuration.saveMode)
      .options(configuration.options)
      .partitionBy(configuration.partitionColumns: _*)
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  def write(data: DataFrame): DataFrame = {
    logInfo(s"Writing data as '${configuration.format}' to '${configuration}'.")
    Try(configureWriter(data, configuration).save()) match {
      case Success(_) =>
        logInfo(s"Successfully saved the data as '${configuration.format}' to '${configuration}'.")
        data
      case Failure(ex) =>
        val message = s"Failed to save the data as '${configuration.format}' to '${configuration}')."
        logError(message)
        throw new DataSinkException(message, ex)
    }
  }
}

/** GenericDataSink trait that is data aware, so it can perform a write call with no arguments */
case class GenericDataAwareSink(configuration: GenericSinkConfiguration, data: DataFrame) extends DataAwareSink[GenericSinkConfiguration, DataFrame] {
  override def sink: DataSink[GenericSinkConfiguration, DataFrame] = GenericDataSink(configuration)
}

/**
 * Output DataFrame sink configuration for Hadoop files.
 * @param format the format can be `csv`, `json`, `orc`, `parquet`, `com.databricks.spark.avro` or just `avro` and
 *               `com.databricks.spark.xml` or just `xml`
 * @param optionalSaveMode the save mode can be `overwrite`, `append`, `ignore` and `error`;
 *                         more details available at [[https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/GenericDataSink.html#mode-java.lang.String-]]
 * @param partitionColumns optionally the writer can layout data in partitions similar to the hive partitions
 * @param buckets optionally the writer can bucket the data, similar to Hive bucketing
 * @param options other sink specific options
 *
 */
case class GenericSinkConfiguration(format: FormatType, optionalSaveMode: Option[String] = None, partitionColumns: Seq[String] = Seq(),
  buckets: Option[BucketsConfiguration] = None,
  options: Map[String, String] = Map())
  extends FormatAwareDataSinkConfiguration {
  def saveMode = optionalSaveMode.getOrElse("default")
  override def toString: String = {
    val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
    s"format: '$format', save mode: '$saveMode', " +
      s"partition columns: [${partitionColumns.mkString(", ")}], " +
      s"bucketing: ${buckets.getOrElse("None")}, " +
      s"options: {$optionsStr}"
  }
}

object GenericSinkConfiguration extends Configurator[GenericSinkConfiguration] with Logging {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._

  implicit val bucketsExtractor = BucketsConfiguration

  def validationNel(config: Config): ValidationNel[Throwable, GenericSinkConfiguration] = {
    config.extract[FormatType]("format") |@|
      config.extract[Option[String]]("mode") |@|
      config.extract[Option[Seq[String]]]("partition.columns").map {
        case (Some(partition_columns)) => partition_columns
        case None => Seq[String]()
      } |@|
      config.extract[Option[BucketsConfiguration]]("buckets") |@|
      config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]())) apply
      GenericSinkConfiguration.apply
  }
}
