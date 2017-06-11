package com.bigdata.kafka.simpleDemo;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.*;
import java.util.Properties;


/**
 * 生产者代码
 *
 * @author lenovo
 */

/*
先场景一个主题,然后开始向这个主题发送数据
./bin/kafka-topics.sh --create --zookeeper jusfoun2016:2181,ncp12:2181,ncp11:2181 --replication-factor 1 --partition 1 --topic localFileToKafka
 */
public class localFile2TestProducer {
    private static Producer<String, String> producer = null;

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "ncp15:6667");
        prop.put("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(prop);
        producer = new Producer<String, String>(producerConfig);

        BufferedReader reader = null;
        String pathRex = "E:\\bigdataData\\tvdata\\2012-09-20\\";
        String[] pathArr = {pathRex + "ars10768@20120920220000.txt", pathRex + "ars10768@20120920220903.txt", pathRex + "ars10768@20120920221500.txt", pathRex + "ars10768@20120920223000.txt",
                pathRex + "ars10768@20120920224500.txt",pathRex + "ars10768@20120920230000.txt",pathRex + "ars10768@20120920231500.txt",};
        for (String path : pathArr) {
            File file = new File(path);
            String temp = "";
            try {
                reader = new BufferedReader(new FileReader(file));
                while ((temp = reader.readLine()) != null) {
                    System.out.println(temp);
                    sendMessage(temp);
                    Thread.sleep(2000);
                }
                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }


    }

    public static void sendMessage(String message) {
        final KeyedMessage<String, String> msg = new KeyedMessage<>("localFileToKafka", message);
        producer.send(msg);
    }


}
