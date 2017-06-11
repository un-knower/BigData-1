package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;

public class KafkaConsumerConfig
{
    public static ConsumerConfig createConfig(String zookeeperString, String groupid)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperString);
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("zookeeper.session.timeout.ms", "3000");

        props.put("offsets.storage", "zookeeper");
        props.put("auto.offset.reset", "smallest");
        props.put("group.id", groupid);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        return consumerConfig;
    }

    public static ConsumerConfig createConfig(Map<String, String> config)
    {
        Properties props = new Properties();
        props.putAll(config);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        return consumerConfig;
    }
}
