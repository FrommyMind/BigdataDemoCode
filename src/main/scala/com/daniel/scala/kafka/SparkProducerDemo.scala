package com.daniel.scala.kafka

import java.util.Properties

import com.daniel.scala.kafka.traits.RDDKafkaWrite
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by daniel on 2020-04-16.
  **/
object SparkProducerDemo extends LazyLogging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val Array (master,brokerlist,topics)= args
//    logger.info(Array (master,brokerlist,topics).toString)
    conf.setMaster(master).setAppName("SparkProducerDemo")
    val sparkContext = new SparkContext(conf)
//    sparkContext.setLogLevel("DEBUG")

    val prop:Properties = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    prop.put("security.protocol", "SASL_PLAINTEXT")
    prop.put("sasl.kerberos.service.name", "kafka")
    prop.put("sasl.mechanism", "GSSAPI")


    val rdd: RDD[Int] = sparkContext.parallelize(1 to 10000)
    val stringRdd: RDD[String] = rdd.map(_.toString)

    val rddKafkaWriter = new RDDKafkaWrite(stringRdd)
    rddKafkaWriter.writeToKafka(prop,s => new ProducerRecord[String, String](topics, s))


    sparkContext.stop()

  }


}
