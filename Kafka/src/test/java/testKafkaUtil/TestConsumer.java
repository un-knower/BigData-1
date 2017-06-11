package testKafkaUtil;


import com.bigdata.kafka.error.DefaultException;
import com.bigdata.kafka.kafkaUtil.*;
import com.bigdata.kafka.redismsg.RedisClusterFactory;
import com.bigdata.kafka.util.ConfigFactory;

import java.util.Properties;

/**
 * 测试 消费者类
 */
public class TestConsumer {

	/**
	 * 用junt 来测试，消费者不会等待 ，所以只能用main函数
	 * @param args
	 * @throws DefaultException 
	 */
	public static void main(String[] args) throws DefaultException {
		TestConsumer consumer = new TestConsumer(); 
		consumer.startConsumer1();
		consumer.startConsumerWithStorm();
	}
	
	public void startConsumer1(){
//		ConfigFactory.init("config/config-kafka.xml");
		ConfigFactory.init("E:\\GitHub\\BigData\\Kafka\\conf\\config-kafka.xml");

		String topicKey = "wzt_topic_key2" ;
		if(RedisClusterFactory.getInstants().getSet(topicKey)==null
				|| RedisClusterFactory.getInstants().getSet(topicKey).size()==0 ){
			RedisClusterFactory.getInstants().add2set( topicKey ,new String[]{"wzt_test3","wzt_test4"});
		}
		
		KafkaConsumer kafkaConsumer = new KafkaConsumer(topicKey   ,
				new KafkaStreamConsumerInterface() {
					public void consume(String topic , String message) {
						System.out.println(topic+"  "+ message); 
					}
		});

		try {
			kafkaConsumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//storm分布式使用
	public void startConsumerWithStorm() throws DefaultException{
//		ConfigFactory.init("config/config-kafka.xml");
		ConfigFactory.init("E:\\GitHub\\BigData\\Kafka\\conf\\config-kafka.xml");
		String topicKey = "wzt_topic_key3" ;
		String topics = ConfigFactory.getString("kafka.defaultTopic") ; 
		String redisUrl = ConfigFactory.getString("redis.url") ; 
		 
		TopicUtil.initTopic(topicKey , topics) ;
		Properties pro = KafkaConfig.getConsumerPro() ; //初始化消费者的属性
		//TopicUtil.getTopic(topicKey  ) ;//初始化topic的信息
		KafkaProducerUtil.initRedis(redisUrl);
		
		KafkaConsumer kafkaConsumer = new KafkaConsumer(topicKey ,  pro, 
				new KafkaStreamConsumerInterface() {
					public void consume(String topic , String message) {
						System.out.println(topic+"  "+ message); 
					}
		});

		try {
			kafkaConsumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	
	
}
