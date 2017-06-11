package testKafkaUtil;


import com.bigdata.kafka.util.ConfigFactory;

public class Test {

	public static void main(String[] args) {
		ConfigFactory.init("config/config-kafka.xml");
		//ConfigFactory.init(Test.class.getResource("/config-kafka.xml").getPath() );
		
//		System.out.println( Test.class.getResource("/config-kafka.xml"));
//		System.out.println( ConfigFactory.getString( "kafka.serializerClass") );  
		
//		Re.setValue( "testtao", "taotao") ;
//		System.out.println( RedisUtils.getValue( "testtao" )  );

		
		
//		String producerVersionKeyTmp =  ConfigFactory.getString("kafka.producerVersionKey") ;
//		String versionKey =  RedisUtils.getValue(producerVersionKeyTmp);
//		System.out.println( RedisUtils.getValue( "kafka.producer.versionKey" )  );
//		System.out.println(versionKey+" == " );

		//RedisUtils.add2set( "wzt_topic_key2",  new String[]{"wzt_test3","wzt_test4"}) ;

//	     Properties props = new Properties();
//	     props.put("bootstrap.servers", "localhost:9092");
//	     props.put("group.id", "test");
//	     props.put("enable.auto.commit", "true");
//	     props.put("auto.commit.interval.ms", "1000");
//	     props.put("session.timeout.ms", "30000");
//	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//	     consumer.subscribe("foo", "bar");
 
		
	}
}
