# FileDataSink


## Description

The `FileDataSink` framework is a utility framework that helps configuring and writing `DataFrames`.

This framework provides for writing to a given path into the specified format like `avro`, `parquet`, `orc`,
`json`, `csv`...

This framework supports different save modes like `overwrite` or `append`, as well as partitioning parameters like
columns and number of partition files.

The framework is composed of two classes:
- `FileDataSink`, which is created based on a `FileSinkConfiguration` class and provides one main function:
    `def write(data: DataFrame): DataFrame`
- `FileSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = FileSinkConfiguration(outputPath, format)
    FileDataSink(sinkConfiguration).write(dataframe)
```

Optionally, one can use the implicit decorator for the `DataFrame` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = FileSinkConfiguration(outputPath, format)
    dataframe.sink(sinkConfiguration).write
```


## Configuration Parameters

- `path` **Required**
  - the path where the result will be saved
- `format` **Required**
  - the format of the output file; acceptable values are `json`, `avro`, `orc` and `parquet`
- `mode` *Optional*
  - the save mode can be `overwrite`, `append`, `ignore` and `error`; more details available
  [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter);
- `partition.columns` *Optional*
  - a sequence of columns that should be used for partitioning data on disk;
  - they should exist in the result of the given sql;
  - if empty no partitioning will be performed.
- `partition.files` *Optional*
  - the number of partition files that will end up in each partition folder;
  - one can always look at the average size of the data inside partition folders and come up with a number that is
    appropriate for the application;
  - for example, one might target a partition number so that the partition file size inside a
    partition folder are around 100 MB
- `options` *Optional*
  - additional options that can be passed to the Apache Spark `DataFrameWriter`;
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file;
  - some useful options, available for *most* of the output formats are:
    - `compression`:  possible values like `none`, `bzip2`, `gzip`, `lz4`, `snappy`
    - `dateFormat`: default `yyyy-MM-dd`
    - `timestampFormat`: default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`
  - more details available [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter);
