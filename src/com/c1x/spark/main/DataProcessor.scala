package com.c1x.spark.main

import java.io.File

import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DataProcessor {
  var parsedConfig = ConfigFactory.parseFile(new File("../../../../main/resources/application.conf"))
  private var conf = ConfigFactory.load(parsedConfig)
  var sparkConf = new SparkConf()
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {import org.apache.spark.streaming.{Seconds, StreamingContext}

      System.err.println(
        s"""
           |Usage: DataPipelineStream <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
                    """.stripMargin)
      System.exit(1)
    }
    //  setting spark conf parameters
    setSparkConfigParams()
    var Array(brokers, topics) = args
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    var topicsSet = topics.split(",").toSet
    var ssc = setupSsc(topicsSet, kafkaParams)

  }

  def setSparkConfigParams() = {
    sparkConf.setAppName(conf.getString("application.app-name"))
    var sparkStreamingConf = conf.getStringList("application.spark-streaming")
    //sparkStreamingConf.forEach { x => val split = x.split("="); sparkConf.set(split(0), split(1)); }
    sparkConf.set("spark.driver.extraJavaOptions",conf.getString("application.event-driver-options"))
    sparkConf.set("spark.executor.extraJavaOptions", conf.getString("application.event-executor-options"))
  }

  def setupSsc(
                topicsSet: Set[String],
                kafkaParams: Map[String, String])(): StreamingContext = {
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(conf.getInt("application.sparkbatchinterval")))
    val zookeeper_host = conf.getString("application.zookeeper_host")
    val kafkaOffsetZookeeperNode = conf.getString("application.kafka_offset_zookeeper_node")
    val messages = createCustomDirectKafkaStream(ssc, kafkaParams, zookeeper_host, kafkaOffsetZookeeperNode, topicsSet)
    val line = messages.map(_._2)
    print(line)
    ssc
  }

  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String, zkPath: String,
                                    topics: Set[String]): InputDStream[(String, String)] = {
    val zkClient = new ZkClient(zkHosts, 30000, 30000)
    //only one time during start up, try to read offset from zookeeper
    val storedOffsets = readOffsets(zkClient, zkHosts, zkPath)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets if not found in zookeeper
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        // start from previously saved offsets if found in zookeeper
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    // save the offsets before processing for each partition of each kafka topic
    //on restart,only this offset range will be re-processed with atleast once, rest others exactly once
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient, zkHosts, zkPath, rdd))
    kafkaStream
  }

  /*
 Read the previously saved offsets of kafka topic partiions from Zookeeper
 e.g. dca-production.event-acc-log-20160530:17:111473
 */
  def readOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String): Option[Map[TopicAndPartition, Long]] = {
    //logger.info("readOffsets: Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        //logger.info(s"readOffsets: Read offset ranges: $offsetsRangesStr")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(topic, partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
          .toMap
        //logger.info("readOffsets: Done reading offsets from Zookeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
        //logger.warn("readOffsets: No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }


  /*
     save offsets of each kakfa partition of each kafka topic to zookeeper
     e.g. dca-production.event-acc-log-20160530:17:111473
   */
  def saveOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String, rdd: RDD[_]): Unit = {
    //logger.info("saveOffsets: Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //offsetsRanges.foreach(offsetRange => logger.info(s"saveOffsets: chandan : Using offsetRange = ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.topic}:${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    //logger.info("saveOffsets: Writing offsets to Zookeeper zkClient=" + zkClient + "  zkHosts=" + zkHosts + "zkPath=" + zkPath + "  offsetsRangesStr:" + offsetsRangesStr)
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    //new ZkUtils(zkClient,new ZkConnection(zkHosts),true).updatePersistentPath( zkPath, offsetsRangesStr)
    //logger.info("saveOffsets: updating offsets in Zookeeper. Took " + stopwatch)
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()

    override def toString = (System.currentTimeMillis - start) + " ms"
  }

}
