package com.c1x.spark.main

import java.io.File

import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
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
    ssc
  }
}
