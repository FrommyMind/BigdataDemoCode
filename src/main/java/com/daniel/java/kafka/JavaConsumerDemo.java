package com.daniel.java.kafka;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by daniel on 2020/4/29.
 **/
public class JavaConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(JavaConsumerDemo.class);
        // TODO: Print out line in log of authenticated user
        System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos");
        System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        String topic = "test1topic";
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"datanode210.daniel.com:9092");
        prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"consumer_test1topic_daniel_group");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"daniel");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        prop.put("security.protocol", "SASL_PLAINTEXT");
        prop.put("sasl.kerberos.service.name", "kafka");
        prop.put("sasl.mechanism", "GSSAPI");
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Collections.singleton(topic));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1000));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println("Received message: (" + consumerRecord.key() + ", " + consumerRecord.value() + ") at offset " + consumerRecord.offset());
        }
    }
}
