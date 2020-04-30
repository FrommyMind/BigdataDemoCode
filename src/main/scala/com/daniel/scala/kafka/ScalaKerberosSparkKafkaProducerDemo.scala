package com.daniel.scala.kafka

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by daniel on 2020-04-16.
 **/
object ScalaKerberosSparkKafkaProducerDemo extends LazyLogging {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val Array(master, brokerList, topics) = args
    logger.info(Array(master, brokerList, topics).toString)
    conf.setMaster(master).setAppName("SparkProducerDemo")
    val sparkContext = new SparkContext(conf)
    //    sparkContext.setLogLevel("DEBUG")
    System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos")
    System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf")
    val prop: Properties = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    prop.setProperty("security.protocol", "SASL_PLAINTEXT")
    prop.setProperty("sasl.kerberos.service.name", "kafka")
    prop.setProperty("sasl.mechanism", "GSSAPI")


    val rdd: RDD[Int] = sparkContext.parallelize(1 to 10000)

    val stringRdd: RDD[String] = rdd.map(_.toString)

    stringRdd.foreachPartition((partition: Iterator[String]) => {
      val producer = new KafkaProducer[String, String](prop)

      partition.map((valueString: String) => new ProducerRecord[String, String](topics, valueString))
        .foreach((record: ProducerRecord[String, String]) => {
          producer.send(record)
          println("Record.key: " + record.key() + ",Record.value: " + record.value())
        })
      producer.flush()
    })

    sparkContext.stop()

  }


}
