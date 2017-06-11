package com.bigdata.kafka.simpleDemo;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;


/**
 * 生产者代码
 *
 * @author lenovo
 */
public class TestProducer {
    private static Producer<String, String> producer = null;

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "192.168.16.165:6667");
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
        final KeyedMessage<String, String> msg = new KeyedMessage<>("flume-kafka", message);
        producer.send(msg);
    }


}
