package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.Map;
import java.util.Properties;
import kafka.producer.ProducerConfig;

public class KafkaProducerConfig
{
    public static ProducerConfig createProducerConfig(String broker_list)
    {
        Properties props = new Properties();

        props.put("metadata.broker.list", broker_list);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.timeout.ms", "10000");
        props.put("producer.type", "async");
        props.put("compression.codec", "snappy");
        props.put("partitioner.class", "com.ctfo.kafka.MyPartitioner");
        ProducerConfig config = new ProducerConfig(props);
        return config;
    }

    public static ProducerConfig createProducerConfig(Properties kafkaprops)
    {
        ProducerConfig config = new ProducerConfig(kafkaprops);
        return config;
    }

    public static ProducerConfig createProducerConfig(Map<String, String> kafkaprops)
    {
        Properties props = new Properties();
        props.putAll(kafkaprops);
        ProducerConfig config = new ProducerConfig(props);
        return config;
    }
}
