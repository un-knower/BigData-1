package testKafkaUtil;

import com.bigdata.kafka.error.DefaultException;
import com.bigdata.kafka.kafkaUtil.KafkaConfig;
import com.bigdata.kafka.kafkaUtil.KafkaProducer;
import com.bigdata.kafka.kafkaUtil.KafkaProducerUtil;
import com.bigdata.kafka.util.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class KafkaProducerTest {
 
	KafkaProducer produer  ;
	//创建生产者的两种方法1 单机使用  2 分布式的storm中使用
	@Before
	public void init() throws DefaultException {
		
//		ConfigFactory.init("config/config-kafka.xml");
		ConfigFactory.init("E:\\GitHub\\BigData\\Kafka\\conf\\config-kafka.xml");
		//1 直接从配置文件中读取生产者的配置，在分布式的strom中不能使用
		//produer = KafkaProducerUtil.getKafkaProducer("wzt_topic_key2" ) ;
		
		//2 预先加载生产者的配置文件,分布式的storm中需要在主函数中先预加载这个配置，
		//保存到conf中，在使用的时候从conf中拿出然后初始化redis并且创建producer
		KafkaConfig.getProducerPro();
//		TopicUtil.loadXmlTopic("wzt_topic_key2") ;//加载conf中的配置的topic
		String redisAddr = ConfigFactory.getString("redis.url"); 
		KafkaProducerUtil.initRedis(redisAddr);
		
		produer = KafkaProducerUtil.getKafkaProducer("wzt_topic_key2"  ) ; 
	}
	
	/**
	 * 发送一个字符串
	 */
	@Test
	public void sendString(){
		try {
			for(int i=0;i<10000;i++ ){
				Thread.sleep(10);
				produer.sendString("key",  i +"=="); 
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * 发送字符串的list
	 */
	@Test
	public void sendStringList( ){
		List<String> list = new ArrayList<String>();
		for(int i=0;i<1000;i++ ){
			list.add("sendStringList" +i ); 
		}
		try {
			produer.sendStringList( list);
			//produer.sendStringList( "sendStringList", list);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void end(){
		System.out.println("关闭了。。");
		//produer.close();
	}
}
