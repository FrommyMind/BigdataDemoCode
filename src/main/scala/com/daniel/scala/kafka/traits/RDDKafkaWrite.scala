package com.daniel.scala.kafka.traits

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD

/**
  * Created by daniel on 2020-04-16.
  **/

class RDDKafkaWrite[T](@transient private  val rdd:RDD[T]) extends KafkaWriter[T] with Serializable {
  /**
    *  Write a [[RDD]] to a Kafka Topic
    * @param producerConfig producer configuration for creating KafkaProducer
    * @param transformFunc a function used to transform values of T type into [[ProducerRecord]]
    * @param callback an optional [[Callback]] to be called after each write, default value is None.
    * @tparam K
    * @tparam V
    */
  override def writeToKafka[K, V](producerProperties:Properties
                                  , transformFunc: T => ProducerRecord[K, V]
                                  , callback: Option[Callback]): Unit = {

    rdd.foreachPartition(partition => {
      val producer = new KafkaProducer[K,V](producerProperties)
      partition.map(transformFunc).foreach(record =>producer.send(record, callback.orNull))
    })
  }
}
