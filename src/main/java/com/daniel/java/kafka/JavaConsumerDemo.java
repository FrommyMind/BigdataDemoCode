package com.daniel.java.kafka;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by daniel on 2020/4/30.
 **/
public class JavaConsumerDemo {

    public static void main(String[] args)  {
        Logger logger = LoggerFactory.getLogger(JavaConsumerDemo.class);
        // TODO: Print out line in log of authenticated user
        String brokerList = null;
        String topic = null;
        if (args.length == 2) {
            brokerList = args[0];
            topic = args[1];
        }
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_test1topic_daniel_group");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "daniel");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                logger.info("Received message: (" + consumerRecord.key() + ", " + consumerRecord.value() + ") at offset " + consumerRecord.offset());
            }
        }
    }

}
