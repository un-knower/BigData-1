package com.bigdata.kafka.kafkaUtil;

import com.bigdata.kafka.error.DefaultException;
import com.bigdata.kafka.redismsg.RedisClusterFactory;
import com.bigdata.kafka.util.ConfigFactory;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kafka的topic的工具类
 * @author admin
 */
public class TopicUtil {
	
	//日志
	public static Logger logger = Logger.getLogger(TopicUtil.class ) ;
	
	// topic的缓存池 
	private static Map<String, Set<String>> topicPool = new ConcurrentHashMap<String, Set<String>>();
	
	/**
	 * desc: 根据actorId获取Topic的数据 ，首先 从Redis中获取  如果拿不到的话从xml中读取默认的Topic
	 * @param actorId  topic在redis中的key 
	 * @return
	 * @throws DefaultException
	 */
	public static Set<String> getTopic(String actorId) throws DefaultException {
		if( topicPool.containsKey(actorId ) && topicPool.get( actorId)!=null ){
			logger.debug("从缓存中获取topic名称=");
			return topicPool.get( actorId) ; 
		}
		Set<String> topicSet ; 
		topicSet = RedisClusterFactory.getInstants().getSet( actorId );
		if(topicSet!=null && topicSet.size()>0){
			logger.debug("从Redis中获取topic名称=");
			topicPool.put(actorId , topicSet) ; 
			return topicSet ; 
		}
		
		topicSet = loadXmlTopic(actorId); 
		return topicSet ;
	}
	
	/**
	 * desc 从配置文件中加载topic数据到存中，使用的时候直接到缓存中读取
	 * 对于分布式如果要从配置文件中读取topic，则需要先初始化这个数据
	 * @param actorId
	 * @throws DefaultException
	 */
	private static Set<String>  loadXmlTopic(String actorId) throws DefaultException{
		String topics = ConfigFactory.getString("kafka.defaultTopic");
		if(topics==null ||  "".equals( topics) ){
			throw new DefaultException("Null Topic Exceipton","No Topic Can find Exception.");
		}
		Set<String> topicSet = MsgStringUtils.arrToSet(topics.split(",")) ;
		logger.debug("从XML中获取topic名称=");
		topicPool.put(actorId , topicSet) ; 
		return topicSet ; 
	}
	
	/**
	 * 初始化Topic信息 
	 * @param actorId   在缓存中存放topic的id 
	 * @param topicSet  从xml中读取的Topic列表，用逗号隔开的字符串 
	 * @return
	 * @throws DefaultException
	 */
	public static Set<String> initTopic(String actorId,String topics ) throws DefaultException{
		if(actorId==null || "".equals( actorId)){
			logger.debug("actorId不能为空=");
			throw new DefaultException("Null actorId Exceipton","No actorId Can find Exception.");
		}
		if( topics==null ||  "".equals( topics) ){
			logger.debug("topics不能为空=");
			throw new DefaultException("Null topics Exceipton","No topics Can find Exception.");
		}
		Set<String> topicSet = MsgStringUtils.arrToSet(topics.split(",")) ;
		topicPool.put(actorId , topicSet) ; 
		return topicSet ;
	}
}
