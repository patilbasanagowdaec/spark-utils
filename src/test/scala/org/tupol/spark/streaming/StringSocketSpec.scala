package org.tupol.spark.streaming

import java.io.PrintStream
import java.net.{ServerSocket, Socket}

import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

/**
 *
 */
trait StringSocketSpec extends BeforeAndAfterAll {
  this: Suite =>

  def port = 9999
  def delayMillis = 1000

  private var _server: ServerSocket = _
  private var _socket: Socket = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _server = Try(new ServerSocket(port)).get
    _socket = Try(_server.accept()).get
  }

  override def afterAll(): Unit = {
    if(_server != null) _server.close
  }


  def send(record: String): Unit = {
    val out = new PrintStream(_socket.getOutputStream())
    out.println(record)
    out.flush()
    Thread.sleep(delayMillis)
  }

  def send(records: Seq[String]): Unit = records.foreach(send)

}
