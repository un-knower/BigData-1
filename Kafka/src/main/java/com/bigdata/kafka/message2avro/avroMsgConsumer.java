package com.bigdata.kafka.message2avro;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者的代码
 * @author lenovo
 *
 */
public class avroMsgConsumer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        //指定zk的地址，通过zk可以获取到kafka的地址
        prop.put("zookeeper.connect", "192.168.16.161:2181,192.168.16.162:2181,192.168.16.163:2181");
        //指定组id
        prop.put("group.id", "test-group");
        ConsumerConfig config = new ConsumerConfig(prop );
        //获取一个java消费者的链接
        ConsumerConnector createJavaConsumerConnector = Consumer.createJavaConsumerConnector(config );
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //指定kafka中的主题，以及每次从主题中获取几条数据
        topicCountMap.put("test-topic", 1);

        //创建一个数据流，从kafka中的指定主题中获取数据
        Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams = createJavaConsumerConnector.createMessageStreams(topicCountMap );

        while(true){
            //从kafka指定主题中获取一条数据流
            KafkaStream<byte[], byte[]> kafkaStream = createMessageStreams.get("test-topic").get(0);
            //迭代
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            if(iterator.hasNext()){
//                String value = new String(iterator.next().message());
//                System.out.println("----------"+value);
//                解析avro格式的消息
                avroStr(iterator.next().message());
            }
        }
    }

    public static void avroStr(byte[] str) {
        Schema schema  = null;
        try {
            schema = new Schema.Parser().parse(new File("Kafka/avsc/test_schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(str, null);
        GenericRecord payload2 = null;
        try {
            payload2 = reader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Message received : " + payload2);
    }

}
