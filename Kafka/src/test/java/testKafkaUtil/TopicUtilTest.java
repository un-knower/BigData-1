package testKafkaUtil;

import com.bigdata.kafka.error.DefaultException;
import com.bigdata.kafka.kafkaUtil.TopicUtil;
import com.bigdata.kafka.util.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class TopicUtilTest {

	@Before
	public  void init(){
//		ConfigFactory.init("config/config-kafka.xml");
		ConfigFactory.init("E:\\GitHub\\BigData\\Kafka\\conf\\config-kafka.xml");

	}
	/**
	 * topic 先从缓存中获取，获取不到从redis中获取，如果获取不到就从xml中获取，还是拿不到就报错
	 */
	@Test
	public void testGetTopic( )  {
		try {
			Set<String> set = TopicUtil.getTopic( "wzt_topic_key4");
			System.out.println( set );
			
		} catch (DefaultException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * 从xml中加载配置的Topic，如果加载不到报错
	 */
	@Test
	public void testLoadXmlTopic( )  {
		try {
			String actorid = "wzt_topic_key";
			String topics = ConfigFactory.getString("kafka.defaultTopic") ; 
			TopicUtil.initTopic(actorid , topics) ;
			//Set<String> set = TopicUtil.loadXmlTopic( "wzt_topic_key2"); 
			Set<String> set2 = TopicUtil.getTopic(actorid);
			 
			System.out.println( set2 );
			
		} catch (DefaultException e) {
			e.printStackTrace();
		} 	
	}
	
}
