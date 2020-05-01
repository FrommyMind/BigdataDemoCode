# BigdataDemoCode

## 目的
大数据相关样例代码
* 包含Java和Scala版本
* 包含启用了Kerberos和未启用Kerberos版本

## 环境
- 大数据集群： 5.16.2-1.cdh5.16.2.p0.8
- Spark： 2.4.0.cloudera2-1.cdh5.13.3.p0.1041012
- Kafka: 3.0.0-1.3.0.0.p0.40
- Flink: 1.9.2-BIN-SCALA_2.12
- JDK: 1.8.0_191
- Scala: 2.11.12
### Spark
1. Spark读取HDFS文件: Java、Scala
2. Spark写数据到HDFS上 ： Java 、Scala
### Flink

### Kafka
1. Kafka生产者：[Java+Kerberos](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/java/com/daniel/java/kafka/JavaKerberosProducerDemo.java)、[Scala+Kerberos](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/scala/com/daniel/scala/kafka/ScalaKerberosProducerDemo.scala)、[Java](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/java/com/daniel/java/kafka/JavaProducerDemo.java)、[Scala](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/scala/com/daniel/scala/kafka/ScalaProducerDemo.scala)
2. Kafka消费者：[Java+Kerberos](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/java/com/daniel/java/kafka/JavaKerberosConsumerDemo.java)、[Scala+Kerberos](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/scala/com/daniel/scala/kafka/ScalaKerberosConsumerDemo.scala)、[Java](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/java/com/daniel/java/kafka/JavaConsumerDemo.java)、[Scala](https://github.com/FrommyMind/BigdataDemoCode/blob/master/src/main/scala/com/daniel/scala/kafka/ScalaConsumerDemo.scala)
### HBase

### Hive

### HDFS