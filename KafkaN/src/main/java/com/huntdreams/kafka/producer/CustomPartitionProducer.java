package com.huntdreams.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * CustomPartitionProducer
 */
public class CustomPartitionProducer {
    private static Producer<String, String> producer;

    public CustomPartitionProducer() {
        Properties props = new Properties();
        // Set the broker list for requesting metadata to find the lead broker
        props.put("metadata.broker.list", "ncp15:6667");
        // This specifies the serializer class for keys
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // Defines the class to be used for determining the partition
        // in the topic where the message needs to be sent.
        props.put("partitioner.class", "com.huntdreams.kafka.partition.SimplePartitioner");
        // 1 means the producer receives an acknowledgment once the lead replica
        // has received the data. This option provides better durability as the
        // client waits until the server acknowledges the request as successful.
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    /*
先创建一个主题,然后开始向这个主题发送数据
./bin/kafka-topics.sh --create --zookeeper jusfoun2016:2181,ncp12:2181,ncp11:2181 --replication-factor 1 --partition 1 --topic log-topic
 ./bin/kafka-console-consumer.sh --zookeeper jusfoun2016:2181,ncp12:2181,ncp11:2181 --topic log-topic --from-beginning
 */
    public static void main(String[] args) {
        String topic = null;
        String count = null;
        int argsCount = args.length;
        if (argsCount == 0) {
            topic = "log-topic";
            count = "100000";
        } else {
            topic = (String) args[0];
            count = (String) args[1];
            throw new IllegalArgumentException(
                    "Please provide topic name and Message count as arguments");
        }
        // Topic name and the message count to be published is passed from the
        // command line
        int messageCount = Integer.parseInt(count);
        System.out.println("Topic Name - " + topic);
        System.out.println("Message Count - " + messageCount);
        CustomPartitionProducer simpleProducer = new CustomPartitionProducer();
        simpleProducer.publishMessage(topic, messageCount);
    }

    private void publishMessage(String topic, int messageCount) {
        Random random = new Random();
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String clientIP = "192.168.14." + random.nextInt(255);
            String accessTime = new Date().toString();
            String message = accessTime + ",kafka.apache.org," + clientIP;
            System.out.println(message);
            // Creates a KeyedMessage instance
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(topic, clientIP, message);
            // Publish the message
            producer.send(data);
        }
        // Close producer connection with broker.
        producer.close();
    }
}
