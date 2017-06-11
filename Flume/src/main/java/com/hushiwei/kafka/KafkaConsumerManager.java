package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.List;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumerManager
{
    private ConsumerConfig config;
    private ConsumerConnector javaConsumerConnector;
    private int numStreams;
    private String topic;

    public KafkaConsumerManager()
    {
    }

    public KafkaConsumerManager(ConsumerConfig config, String topic, int numStreams)
    {
        this.config = config;
        this.topic = topic;
        this.numStreams = numStreams;
    }

    public List<KafkaStream<byte[], byte[]>> createKafkaStreams() {
        this.javaConsumerConnector = Consumer.createJavaConsumerConnector(this.config);

        Whitelist whitelist = new Whitelist(this.topic);
        List streams = this.javaConsumerConnector.createMessageStreamsByFilter(
                whitelist, this.numStreams);
        return streams;
    }

    public ConsumerConfig getConfig() {
        return this.config;
    }

    public ConsumerConnector getJavaConsumerConnector() {
        return this.javaConsumerConnector;
    }

    public int getNumStreams() {
        return this.numStreams;
    }

    public String getTopic() {
        return this.topic;
    }

    public void setConfig(ConsumerConfig config) {
        this.config = config;
    }

    public void setJavaConsumerConnector(ConsumerConnector javaConsumerConnector) {
        this.javaConsumerConnector = javaConsumerConnector;
    }

    public void setNumStreams(int numStreams) {
        this.numStreams = numStreams;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void stop() {
        this.javaConsumerConnector.shutdown();
    }
}
