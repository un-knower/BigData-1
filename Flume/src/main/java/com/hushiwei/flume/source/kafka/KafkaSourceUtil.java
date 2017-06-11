package com.hushiwei.flume.source.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceUtil
{
    private static final Logger log = LoggerFactory.getLogger(KafkaSourceUtil.class);

    public static Properties getKafkaProperties(Context context) {
        log.info("context={}", context.toString());
        Properties props = generateDefaultKafkaProps();
        setKafkaProps(context, props);
        addDocumentedKafkaProps(context, props);
        return props;
    }

    public static ConsumerConnector getConsumer(Properties kafkaProps) {
        ConsumerConfig consumerConfig =
                new ConsumerConfig(kafkaProps);
        ConsumerConnector consumer =
                Consumer.createJavaConsumerConnector(consumerConfig);
        return consumer;
    }

    private static Properties generateDefaultKafkaProps()
    {
        Properties props = new Properties();
        props.put("auto.commit.enable",
                "false");
        props.put("consumer.timeout.ms",
                "10");
        props.put("group.id",
                "flume");
        return props;
    }

    private static void setKafkaProps(Context context, Properties kafkaProps)
    {
        ImmutableMap<String, String> kafkaProperties =
                context.getSubProperties("kafka.");

        for (Map.Entry prop : kafkaProperties.entrySet())
        {
            kafkaProps.put(prop.getKey(), prop.getValue());
            if (log.isDebugEnabled())
                log.debug("Reading a Kafka Producer Property: key: " +
                        (String)prop.getKey() + ", value: " + (String)prop.getValue());
        }
    }

    private static void addDocumentedKafkaProps(Context context, Properties kafkaProps)
            throws ConfigurationException
    {
        String zookeeperConnect = context.getString(
                "zookeeperConnect");
        if (zookeeperConnect == null) {
            throw new ConfigurationException("ZookeeperConnect must contain at least one ZooKeeper server");
        }

        kafkaProps.put("zookeeper.connect", zookeeperConnect);

        String groupID = context.getString("groupId");

        if (groupID != null)
            kafkaProps.put("group.id", groupID);
    }
}
