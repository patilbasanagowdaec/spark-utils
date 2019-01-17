package org.tupol.spark.streaming

import org.apache.spark.sql.Row

package object mocks {

  def dummyFun(in: String) = s"$in-transformed-by-mock-function"

  case class Line(line: String) extends AnyVal

  case class TestTopicRecord(c1field: String, c2field: String, c3field: Long, c4field: Boolean)

  object TestTopicRecord {
    def apply(row: Row): TestTopicRecord = TestTopicRecord(row.getAs[String]("c1field"), row.getAs[String]("c2field"),
      row.getAs[Long]("c3field"), row.getAs[Boolean]("c4field"))
  }

}
