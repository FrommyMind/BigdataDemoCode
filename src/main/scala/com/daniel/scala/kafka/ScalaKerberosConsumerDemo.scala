package com.daniel.scala.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import com.daniel.java.kafka.JavaConsumerDemo
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by daniel on 2020/4/30.
 **/
object ScalaKerberosConsumerDemo {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(ScalaKerberosConsumerDemo.getClass)
    // TODO: Print out line in log of authenticated user
    System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos")
    System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    val topic = "test1topic"
    val prop = new Properties
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "datanode210.daniel.com:9092")
    prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_test1topic_daniel_group")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "daniel")
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    prop.put("security.protocol", "SASL_PLAINTEXT")
    prop.put("sasl.kerberos.service.name", "kafka")
    prop.put("sasl.mechanism", "GSSAPI")
    val consumer = new KafkaConsumer[String, String](prop)

    consumer.subscribe(Collections.singleton(topic))
    val consumerRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1000))
    import scala.collection.JavaConversions._
    for (consumerRecord <- consumerRecords) {
      System.out.println("Received message: (" + consumerRecord.key + ", " + consumerRecord.value + ") at offset " + consumerRecord.offset)
    }
  }

}
