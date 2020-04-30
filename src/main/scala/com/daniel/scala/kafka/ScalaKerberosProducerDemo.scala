package com.daniel.scala.kafka

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by daniel on 2020-04-15.
  **/
object ScalaKerberosProducerDemo extends LazyLogging{

  def main(args: Array[String]): Unit = {

    // TODO: Print out line in log of authenticated user
    val logger: Logger = LoggerFactory.getLogger(ScalaKerberosProducerDemo.getClass)
    System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos")
    System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf")

    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
//    System.setProperty("sun.security.krb5.debug", "true")
    val Array(brokerlist, topics) = args
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerlist)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    prop.put("security.protocol", "SASL_PLAINTEXT")
    prop.put("sasl.kerberos.service.name", "kafka")
    prop.put("sasl.mechanism", "GSSAPI")

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
