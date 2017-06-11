package com.bigdata.kafka.kafkaUtil;

import com.bigdata.kafka.error.DefaultException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @ClassName: KafkaProducer 
 * @Description: kafka 的生产者
 * @date 2016年3月24日
 *
 * 从redis中取topic
 */
public class KafkaProducer {
	
	private static Logger logger = Logger.getLogger(KafkaProducer.class ) ; 
	
	private Producer<String, String> producer;
	private Set<String> topics = new HashSet<String>(); 
 
	//topicKey 获取topic 的key值 
	public KafkaProducer(String topicKey ) {
		try {
			topics.addAll(TopicUtil.getTopic(topicKey ) ) ;
		} catch (DefaultException e) {
			e.printStackTrace();
		}
		logger.debug("生产者中获取的Topic信息为"+topics);
		producer = createProducer() ;
	}

	//topicKey 获取topic 的key值 
	public KafkaProducer(String topicKey,Properties producerPro) {
		try {
			topics.addAll(TopicUtil.getTopic(topicKey ) ) ;
		} catch (DefaultException e) {
			e.printStackTrace();
		}
		producer = createProducer(producerPro) ;
	}
 
	//创建生产者
	private  Producer<String,String>  createProducer(){
		return new Producer<String,String>(new ProducerConfig(KafkaConfig.getProducerPro()));
	}

	//创建生产者
	private  Producer<String,String>  createProducer(Properties producerPro){
		return new Producer<String,String>(new ProducerConfig(producerPro));
	}
	
	//发送字符串，有key 
	public void sendString(String key ,String msg)  {
		if(topics!=null && topics.size()>0){
			for(String topic : topics ){
				producer.send(buildKeyedMessage(topic,key,msg));	
			}
		}
	}
	
	//发送字符串
	public void sendString(String msg)  {
		if(topics!=null && topics.size()>0){
			for(String topic : topics ){
				producer.send(buildKeyedMessage(topic ,msg));	
			}
		}
	}
	
	//创建keyedMessage，需要key
	private KeyedMessage<String, String> buildKeyedMessage(String topic,String key , String msg){
		return new KeyedMessage<String, String>(topic, key ,  msg);
	}

	//创建keyedMessage 没有key
	private KeyedMessage<String, String> buildKeyedMessage(String topic, String msg){
		return new KeyedMessage<String, String>(topic,msg);
	}
	
	//发送N个字符串,这个方法不支持异步发送数据（测试发现）
	public void sendStringList(List<String> msgList)
			throws Exception {
		List<KeyedMessage<String, String>> keyedMessageList ;
		if(topics!=null && topics.size()>0){
			for(String topic : topics ){
				keyedMessageList= new ArrayList<KeyedMessage<String, String>>();
				for (String msg : msgList)
					keyedMessageList.add(buildKeyedMessage(topic , msg));
				producer.send(keyedMessageList);
			}
		}
	}
	
	//发送N个字符串, 这个方法不支持异步发送数据（测试发现）
	public void sendStringList(String key, List<String> msgList)
			throws Exception {
		List<KeyedMessage<String, String>> keyedMessageList = new ArrayList<KeyedMessage<String, String>>();
		if(topics!=null && topics.size()>0){
			for(String topic : topics ){
				//keyedMessageList
				for (String msg : msgList)
					keyedMessageList.add(buildKeyedMessage(topic ,key , msg));
			}
			producer.send(keyedMessageList);
		}
	}
	
	//关闭
	public void close() {
		producer.close();
	}
	
	
	public Set<String> getTopics() {
		return topics;
	}

	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public static void main(String[] args) throws InterruptedException {


	}
}
