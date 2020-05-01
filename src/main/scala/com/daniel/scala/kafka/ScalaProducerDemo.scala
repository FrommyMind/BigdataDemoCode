package com.daniel.scala.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by daniel on 2020/4/30.
 **/
object ScalaProducerDemo {
  def main(args: Array[String]): Unit = {
    // TODO: Print out line in log of authenticated user
    val logger: Logger = LoggerFactory.getLogger(ScalaProducerDemo.getClass)
    val Array(brokerList, topics) = args

    val prop: Properties = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    logger.info("args are :" + args.toString)

    val producer = new KafkaProducer[String,String](prop)
    for (i <- 0 to 100000) {
      producer.send(new ProducerRecord[String,String](topics, "hello "+i))
      logger.info("Key: " + i + "-> Value: " + "hello" +i )
    }
    producer.flush()
    producer.close()

  }

}
