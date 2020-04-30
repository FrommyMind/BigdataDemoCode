package com.daniel.scala.kafka.traits

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}


/**
  * Created by daniel on 2020-04-16.
  **/
trait KafkaWriter[T] extends Serializable {
  def writeToKafka[K,V](
                         producerProperties:Properties,
                         transformFunc: T => ProducerRecord[K, V],
                         callback: Option[Callback] = None
                       ):Unit
}
