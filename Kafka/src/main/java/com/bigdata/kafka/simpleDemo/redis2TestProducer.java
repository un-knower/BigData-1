package com.bigdata.kafka.simpleDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import redis.clients.jedis.Jedis;


/**
 * 生产者代码
 * @author lenovo
 *
 */
public class redis2TestProducer {
	private static Producer<String, String> producer = null;

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("metadata.broker.list", "192.168.4.202:6667");
		prop.put("serializer.class", StringEncoder.class.getName());
		ProducerConfig producerConfig = new ProducerConfig(prop );
		producer = new Producer<String,String>(producerConfig);

		String ip = "192.168.4.202";
		int port = 6379;
		final Jedis jedis = new Jedis(ip, port);
		final List<String> list = jedis.lrange("externalService.mysqljob.mysqlnode.outputData", 0, -1);
		for (String s : list) {
			sendMessage(s);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


	}

	public static void sendMessage(String message) {
		final KeyedMessage<String, String> msg = new KeyedMessage<>("redisToKafka", message);
		producer.send(msg);
	}


}
