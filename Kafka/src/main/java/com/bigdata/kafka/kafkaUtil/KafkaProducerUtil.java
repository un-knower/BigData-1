package com.bigdata.kafka.kafkaUtil;


import com.bigdata.kafka.redismsg.RedisClusterFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kafka的缓冲池
 * @author admin
 *
 */
public class KafkaProducerUtil {
	
	//static Map<String, Producer<String, String>> kafkaProducerPool = new ConcurrentHashMap<>();
	static Map<String, KafkaProducer> kafkaProducerPool = new ConcurrentHashMap<>();

	private static void addKafkaProducer(String actorId,  KafkaProducer producer){
		kafkaProducerPool.put(actorId, producer);
	}
	
	public static void closeAndRemoveKafkaProducer(String actorId){
		KafkaProducer kafkaProducer = kafkaProducerPool.remove(actorId);
		if(kafkaProducer.getProducer() != null){
			kafkaProducer.close();
			kafkaProducer.setProducer(null);
			kafkaProducer=null; 
		}
	}
	
	/**
	 * 
	 * @param actorId 1 缓冲池中的 生产者的key  2 redis中获得topic的key 
	 * @return
	 */
	public static synchronized KafkaProducer getKafkaProducer(String actorId ){
		KafkaProducer kafkaProducer = kafkaProducerPool.get(actorId);
		 
		if(kafkaProducer == null){
			kafkaProducer = new KafkaProducer( actorId);
			addKafkaProducer(actorId, kafkaProducer);
		} else {
			//System.out.println("### refreshProducer for actorId: " + actorId);
			//kafkaProducer.close(); kafkaProducer = null;
			kafkaProducer = new KafkaProducer( actorId);
			addKafkaProducer(actorId, kafkaProducer);
		}
		return  kafkaProducer;
	}
	
	//初始化redis中的配置
	public static  void initRedis( String addr){
		RedisClusterFactory.initRedisCluster(addr );
	}
	
	/**
	 * 
	 * @param actorId 1 缓冲池中的 生产者的key  2 redis中获得topic的key 
	 * @return
	 */
	public static synchronized KafkaProducer getKafkaProducer(String actorId ,Properties producerPro  ){
		KafkaProducer kafkaProducer = kafkaProducerPool.get(actorId);
		 
		if(kafkaProducer == null){
			kafkaProducer = new KafkaProducer( actorId);
			addKafkaProducer(actorId, kafkaProducer);
		} else {
			kafkaProducer = new KafkaProducer( actorId , producerPro); 
			addKafkaProducer(actorId, kafkaProducer); 
		}
		return  kafkaProducer;
	}
	
	
	
	public static void closeAllProducers(){
		Collection<KafkaProducer> producers = kafkaProducerPool.values();
		for (KafkaProducer kafkaProducer : producers) {
			kafkaProducer.close(); 
			kafkaProducer.setProducer(null);
			kafkaProducer = null;
		}
	}
}
