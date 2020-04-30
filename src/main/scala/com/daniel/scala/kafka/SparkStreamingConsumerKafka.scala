package com.daniel.scala.kafka

import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/** an simple example of just consumer data from kafka topic, and print some of them.
  *
  * Created by daniel on 2020-04-17.
  **/
object SparkStreamingConsumerKafka {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkStreamingConsumerKafka")
        sparkConf.setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    //    sc.setLogLevel("DEBUG")

    val scc: StreamingContext = new StreamingContext(sc, Seconds(3)) //创建sparkstreaming
    val Array(principal, keyTab) = args
    // login in
    UserGroupInformation.loginUserFromKeytab(principal, keyTab)
    val topics: Set[String] = Array("test1topic").toSet
    System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos")
    System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf")
    val random = new Random()
    val group:String = "consumer_group" + random.nextInt(10000)
    val kafkaParams:Map[String, Object]  = Map[String, Object](
      "auto.offset.reset" -> "earliest" //latest,earliest
      , "value.deserializer" -> classOf[StringDeserializer]
      , "key.deserializer" -> classOf[StringDeserializer]
      , "bootstrap.servers" -> "datanode210.daniel.com:9092"
      , "group.id" -> group
      , "enable.auto.commit" -> (false: java.lang.Boolean)
      , "security.protocol" -> "SASL_PLAINTEXT"
      , "sasl.kerberos.service.name" -> "kafka"
      , "sasl.mechanism" -> "GSSAPI"
    )

    var stream: InputDStream[ConsumerRecord[String, String]] = null

    stream = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    println("Start.......................................stream__________")

    stream.map(println)
    println("Start.......................................stream__________")
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("Rdd is not null .......................................")
        //获取当前批次的RDD的偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val cache_rdd: RDD[String] = rdd.map(_.value())
        val top10: Array[String] = cache_rdd.take(10)
        val cnt:Long = cache_rdd.count()
        println("---------------------------------------------接收数据")
        top10.foreach(println)

        println("总计:" + cnt)
        println("-----接收完成-----")
        //提交当前批次的偏移量，偏移量最后写入kafka
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } else {
        println("Rdd is null _______--------------------________________")
      }
    })
    scc.start()
    //用于等待子线程结束，再继续执行下面的代码。
    scc.awaitTermination()
  }


}

/** 提交命令
export SPARK_KAFKA_VERSION=0.10; \
spark2-submit --class com.daniel.kafka.SparkStreamingConsumerKafka\
--name "test" \
--master yarn \
--deploy-mode cluster \
--executor-memory 2g \
--executor-cores 2 \
--driver-memory 2g \
--num-executors 2 \
--files /root/daniel_jaas.conf,/root/daniel.keytab \
--driver-java-options "-Djava.security.auth.login.config=./daniel_jaas.conf" \
--conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./daniel_jaas.conf" \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./daniel_jaas.conf" \
/root/democode-1.0-SNAPSHOT.jar daniel@DANIEL.COM ./daniel.keytab

  为了保持任务长时间运行，需要给daniel的principal renew属性

其中 daniel_jaas.conf的内容是

  KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="./daniel.keytab"
  serviceName="kafka"
  principal="daniel@DANIEL.COM";
};


Client {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="./daniel.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="zookeeper"
   principal="daniel@DANIEL.COM";
};


  **/