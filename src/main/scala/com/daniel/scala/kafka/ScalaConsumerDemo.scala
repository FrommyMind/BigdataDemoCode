package com.daniel.scala.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by daniel on 2020/4/30.
 **/
object ScalaConsumerDemo {
  def main(args: Array[String]): Unit = {


    val logger: Logger = LoggerFactory.getLogger(ScalaConsumerDemo.getClass)
    // TODO: Print out line in log of authenticated user
    import scala.collection.JavaConversions._
    val Array(brokerList, topic) = args
    val prop: Properties = new Properties
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_test1topic_daniel_group")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "daniel")
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

    val consumer = new KafkaConsumer[String, String](prop)
    consumer.subscribe(Collections.singleton(topic))
    //    logger.info("Topics {} has {} partitions",topic , consumer.listTopics().);
    while (true) {
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1000))

      for (consumerRecord <- consumerRecords) {
        System.out.println("Received message: (" + consumerRecord.key + ", " + consumerRecord.value + ") at offset " + consumerRecord.offset)
      }
    }

  }
}
