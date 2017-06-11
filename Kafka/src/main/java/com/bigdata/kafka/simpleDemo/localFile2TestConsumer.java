package com.bigdata.kafka.simpleDemo;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者的代码
 * @author lenovo
 *
 */
public class localFile2TestConsumer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        //指定zk的地址，通过zk可以获取到kafka的地址
        prop.put("zookeeper.connect", "jusfoun2016:2181,ncp12:2181,ncp11:2181");
        //指定组id
        prop.put("group.id", "group11");
        ConsumerConfig config = new ConsumerConfig(prop );
        //获取一个java消费者的链接
        ConsumerConnector createJavaConsumerConnector = Consumer.createJavaConsumerConnector(config );
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //指定kafka中的主题，以及每次从主题中获取几条数据
        topicCountMap.put("localFileToKafka", 1);

        //创建一个数据流，从kafka中的指定主题中获取数据
        Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams = createJavaConsumerConnector.createMessageStreams(topicCountMap );

        while(true){
            //从kafka指定主题中获取一条数据流
            KafkaStream<byte[], byte[]> kafkaStream = createMessageStreams.get("localFileToKafka").get(0);
            //迭代
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            if(iterator.hasNext()){
                String value = new String(iterator.next().message());
                System.out.println(value);
                //把接收到的原始内容保存到hbase数据库中
                //TODO--
            }
        }
    }

}
