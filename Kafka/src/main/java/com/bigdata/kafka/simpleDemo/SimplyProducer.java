package com.bigdata.kafka.simpleDemo;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;


/**
 * 生产者代码
 *
 * kafka-console-producer --broker-list 10.10.25.13:9092 --topic my_test
 * @author lenovo
 */
public class SimplyProducer {
    private static Producer<String, String> producer = null;

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "10.10.25.13:9092");
        prop.put("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(prop);
        producer = new Producer<String, String>(producerConfig);

        while (true) {
            sendMessage("hello world");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void sendMessage(String message) {
        final KeyedMessage<String, String> msg = new KeyedMessage<>("my_test", message);
        producer.send(msg);
    }


}
