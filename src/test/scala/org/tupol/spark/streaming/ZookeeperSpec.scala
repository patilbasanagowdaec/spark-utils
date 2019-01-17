package org.tupol.spark.streaming

import kafka.utils.{TestZKUtils, ZKStringSerializer}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 *
 */
trait ZookeeperSpec extends BeforeAndAfterAll {
  this: Suite =>

  private var _zkServer: EmbeddedZookeeper = _
  private var _zkClient: ZkClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // setup Zookeeper
    val zkConnect = TestZKUtils.zookeeperConnect
    _zkServer = new EmbeddedZookeeper(zkConnect)
    _zkClient = new ZkClient(zkServer.connectString, 30000, 30000, ZKStringSerializer)
  }

  override def afterAll(): Unit = {
    if (_zkClient != null) {
      _zkClient.close()
      _zkClient = null
    }

    if (_zkServer != null) {
      _zkServer.shutdown()
      _zkServer = null
    }

    super.afterAll()
  }

  def zkServer: EmbeddedZookeeper = _zkServer
  def zkClient: ZkClient = _zkClient

}
