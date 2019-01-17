package org.tupol.spark.streaming

import java.util.Properties

import kafka.admin.TopicCommand
import kafka.consumer._
import kafka.producer.{ Producer, ProducerConfig }
import kafka.serializer.StringDecoder
import kafka.server.{ KafkaConfig, KafkaServer }
import kafka.utils.{ MockTime, TestUtils }
import org.scalatest.{ BeforeAndAfterAll, Suite }

/**
 *
 */
trait KafkaSpec extends BeforeAndAfterAll with ZookeeperSpec {
  this: Suite =>

  val brokerId = 0
  val topicInput = "test-input"
  val topicOutput = "test-output"

  private var _producer: Producer[String, String] = _
  private var _consumer: ConsumerConnector = _
  private var _consumerStream: KafkaStream[String, String] = _
  private var _kafkaServer: KafkaServer = _
  private var _kafkaPort: Integer = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // setup Broker
    _kafkaPort = TestUtils.choosePort()
    val enableControlledShutdown: Boolean = true
    val props: Properties = TestUtils.createBrokerConfig(brokerId, _kafkaPort, enableControlledShutdown)

    val config: KafkaConfig = new KafkaConfig(props)
    val mock: MockTime = new MockTime()
    _kafkaServer = TestUtils.createServer(config, mock)

    val argumentsIn: Array[String] = Array("--topic", topicInput, "--partitions", "1", "--replication-factor", "1")
    // create the input topic
    TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(argumentsIn))

    val argumentsOut: Array[String] = Array("--topic", topicOutput, "--partitions", "1", "--replication-factor", "1")
    // create the output topic
    TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(argumentsOut))

    val servers: Seq[KafkaServer] = Seq(_kafkaServer)
    TestUtils.waitUntilMetadataIsPropagated(servers, topicInput, 0, 5000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topicOutput, 0, 5000)

    // setup topicInput producer
    val properties: Properties = TestUtils.getProducerConfig("localhost:" + _kafkaPort)
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    val producerConfig: ProducerConfig = new ProducerConfig(properties)
    _producer = new Producer[String, String](producerConfig)

    // setup topicOutput consumer
    // In Kafka 0.9.x look for TestUtils.createNewConsumer()
    val consumerProps = TestUtils.createConsumerProperties(zkServer.connectString, "testGroupId", "testConsumerId", 5000)
    _consumer = Consumer.create(new ConsumerConfig(consumerProps))
    val topicFilter = new Whitelist(topicOutput)
    _consumerStream = _consumer.createMessageStreamsByFilter(topicFilter, 1, new StringDecoder, new StringDecoder)(0)

  }

  override def afterAll(): Unit = {

    if (_producer != null) {
      _producer.close()
      _producer = null
    }
    if (_consumer != null) {
      _consumer.shutdown()
      _consumer = null
    }
    if (_kafkaServer != null) {
      _kafkaServer.shutdown()
      _kafkaServer = null
    }
    super.afterAll()
  }

  def producer: Producer[String, String] = _producer
  def consumer: ConsumerConnector = _consumer
  def outputConsumerStream: KafkaStream[String, String] = _consumerStream

  def kafkaServer: KafkaServer = _kafkaServer
  def kafkaPort: Integer = _kafkaPort
}
