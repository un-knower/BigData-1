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
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


/**
 * 生产者代码
 *
 * @author lenovo
 */
public class TestProducerMsgAvro {
    private static Producer<String, byte[]> producer = null;

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "192.168.16.165:6667");
        prop.put("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(prop);
        producer = new Producer<String, byte[]>(producerConfig);

        while (true) {
            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("hello", "world");
            sendMessage(hashMap);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void sendMessage(HashMap<String, String> message) {
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("Kafka/avsc/test_schema.avsc"));
            GenericRecord record = new GenericData.Record(schema);
            record.put("data", message);
            record.put("desc", "ThisIsDesc");
            record.put("id", 110);
            System.out.println("Original Message : " + record);
            byte[] serializedBytes = serializeEvent(record);
            final KeyedMessage<String, byte[]> msg = new KeyedMessage<>("flume-kafka", serializedBytes);
            producer.send(msg);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] serializeEvent(GenericRecord record) throws Exception {
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
            writer.write(record, encoder);
            encoder.flush();
            byte[] serializedValue = bos.toByteArray();
            return serializedValue;
        } catch (Exception ex) {
            throw ex;
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (Exception e) {
                    bos = null;
                }
            }
        }
    }

}
