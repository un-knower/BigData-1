package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer
{
    private Producer<byte[], byte[]> producer;
    private String topic;
    byte[] value = null;

    public KafkaProducer(String topic, ProducerConfig config) {
        this.topic = topic;
        this.producer = new Producer(config);
    }

    public void send(byte[] messageByteArray)
    {
        KeyedMessage km = new KeyedMessage(this.topic, messageByteArray);
        this.producer.send(km);
    }

    public void sendKeyMessage(byte[] key, byte[] messageByteArray)
    {
        KeyedMessage km = new KeyedMessage(this.topic, key, messageByteArray);
        this.producer.send(km);
    }

    public void close() {
        if (this.producer != null)
            this.producer.close();
    }
}
