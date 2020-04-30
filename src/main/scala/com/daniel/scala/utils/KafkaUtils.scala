package com.daniel.scala.utils

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Created by daniel on 2020-04-16.
  **/
object KafkaUtils {

  def loadKafkaProducerProperties(prop:Properties,brokerlist:String): Unit ={
    System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos")
    System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/Bigdata04/democode/src/main/resources/jaas.conf")

    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    //    System.setProperty("sun.security.krb5.debug", "true")
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerlist)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    prop.put("security.protocol", "SASL_PLAINTEXT")
    prop.put("sasl.kerberos.service.name", "kafka")
    prop.put("sasl.mechanism", "GSSAPI")
  }
}
