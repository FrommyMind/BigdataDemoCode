package com.daniel.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by daniel on 2020/4/29.
 **/
public class JavaProducerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(JavaProducerDemo.class);
        // TODO: Print out line in log of authenticated user
        System.setProperty("java.security.krb5.conf", "/Library/Preferences/edu.mit.Kerberos");
        System.setProperty("java.security.auth.login.config", "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//    System.setProperty("sun.security.krb5.debug", "true")

        String brokerlist = null;
        String topics = null;
        if (args.length == 2) {
            brokerlist = args[0];
            topics = args[1];
        }
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put("security.protocol", "SASL_PLAINTEXT");
        prop.put("sasl.kerberos.service.name", "kafka");
        prop.put("sasl.mechanism", "GSSAPI");

        logger.info("args are :" + Arrays.toString(args));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        for (int i = 0; i < 100000; i++) {
            assert topics != null;
            producer.send(new ProducerRecord<String, String>(topics, "hello " + i));
            logger.info("Key: " + i + "-> Value: " + "hello" + i);
        }
        producer.flush();
        producer.close();

    }
}
