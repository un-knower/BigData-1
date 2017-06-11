package com.bigdata.kafka.kafkaUtil;

import com.bigdata.kafka.error.DefaultException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 
 * @ClassName: KafkaConsumer 
 * @Description: Kafka 的消费者 
 * @date 2016年3月24日
 *
 */
public class KafkaConsumer {

	public static Logger logger = Logger.getLogger(KafkaConsumer.class ) ;
	
	private static final String KAFKA_CONSUMING_WORKER =
			"-kafkaConsumingWorker-";
	//kafka 的所有配置信息
	private ConsumerConfig consumerConfig;
	//
	private ConsumerConnector kafkaConsumerConnector;
	//消费者消费的topic信息
	private Map<String, Integer> topicInfo;
	//连接池 每个topic会开启一个线程消费
	private ExecutorService kafkaConsumingThreadPool;
	//消费后的信息 回调接口 
	private KafkaStreamConsumerInterface kafkaStreamWork;
	 
	/**
	 * @param topicKey 获取topic的 key值，这个是在Redis中定义的
	 * @param kafkaStreamWork 回调方法
	 */
	public KafkaConsumer( String topicKey ,
			KafkaStreamConsumerInterface kafkaStreamWork) {
		
		//topicInfo  通过 logSystem ，在 redis中读取 
		initTopic(topicKey);
		//设置回调 方法 
		setKafkaStreamWork(kafkaStreamWork);
		consumerConfig = getConsumerConfig() ;
	}

	/**
	 * @param topicKey 获取topic的 key值，这个是在Redis中定义的
	 * @param kafkaStreamWork 回调方法
	 */
	public KafkaConsumer( String topicKey, Properties consumerPro ,
			KafkaStreamConsumerInterface kafkaStreamWork) {
		
		//topicInfo  通过 logSystem ，在 redis中读取 
		initTopic(topicKey);
		//设置回调 方法 
		setKafkaStreamWork(kafkaStreamWork);
		consumerConfig = getConsumerConfig( consumerPro ) ;
	}
	
	/**
	 * 
	* @Title: initTopic 
	* @Description: 初始化topic信息  
	* @param @param logSystem    设定文件 
	* @return void    返回类型 
	* @throws
	 */
	private void initTopic(String topicKey ){// JCFSYSTEM logSystem
		Set<String> topicSet = null;
		try {
			topicSet = TopicUtil.getTopic(topicKey );
		} catch (DefaultException e) {
			e.printStackTrace();
		}
		logger.debug("获取的Topic信息="+topicSet);
		topicInfo = new HashMap<String,Integer>();
		Iterator<String> it = topicSet.iterator() ;
		while(it.hasNext()){
			topicInfo.put( it.next(),new Integer(1)) ; 
		}
	}
	
	///读取redis获取配置信息 
	private  ConsumerConfig getConsumerConfig(){
        return new ConsumerConfig( KafkaConfig.getConsumerPro() );
	}
	//通过给定的参数初始化 消费者配置 
	private  ConsumerConfig getConsumerConfig(Properties consumerPro){
        return new ConsumerConfig( consumerPro );
	}
	
	public void start() throws Exception {
		this.kafkaConsumerConnector =  Consumer
				.createJavaConsumerConnector(consumerConfig);
		
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams =
				kafkaConsumerConnector.createMessageStreams(topicInfo );
		kafkaConsumingThreadPool =
				Executors.newFixedThreadPool(messageStreams.size());
		
		for (String topic : topicInfo.keySet()) {
			List<KafkaStream<byte[], byte[]>> kafkaStreamList =
					messageStreams.get(topic);
 
			int streamNum = 0;
			
			for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreamList) {
				kafkaConsumingThreadPool.submit(
						new ConsumingWorker(topic, ++streamNum, kafkaStream));
				logger.info("={}" + KAFKA_CONSUMING_WORKER + "{} Running !!! topic信息= " +topic+"  流信息="+streamNum);
			}
		}
	}

	public void startAsDeamon() throws Exception {
		start();
		while (!kafkaConsumingThreadPool.isTerminated())
			Thread.sleep(10000);
	}

	public boolean isRunning() {
		return !kafkaConsumingThreadPool.isTerminated();
	}

	public void stop() throws RuntimeException {
		if (kafkaConsumerConnector != null)
			kafkaConsumerConnector.shutdown();
		if (kafkaConsumingThreadPool != null)
			kafkaConsumingThreadPool.shutdown();
		while (!kafkaConsumingThreadPool.isTerminated()) {
		}
	}

	public void setKafkaStreamWork(KafkaStreamConsumerInterface kafkaStreamWork) {
		this.kafkaStreamWork = kafkaStreamWork;
	}

	private class ConsumingWorker implements Runnable {

		private String topic;
		private int streamNum;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private String threadName;

		public ConsumingWorker(String topic, int numOfThreadForKafka,
				KafkaStream<byte[], byte[]> kafkaStream) {
			this.topic = topic;
			this.streamNum = numOfThreadForKafka;
			this.kafkaStream = kafkaStream;
			this.threadName = topic + KAFKA_CONSUMING_WORKER + streamNum;
		}
		
		@Override
		public void run(){
			try {
				//System.out.println("===================="+kafkaStream.size() );
				Thread.currentThread().setName(threadName);
				ConsumerIterator<byte[], byte[]> consumerIterator =
						kafkaStream.iterator();
				while (consumerIterator.hasNext()) {
					String message = new String( consumerIterator.next().message() ); 
					logger.debug( "consuming topic = {}, streamNum = {}, messageBytes = {}"+
							topic+  streamNum+ message.length() );
					try {
						//System.out.println(topic+"==="+ message );
						kafkaStreamWork.consume(topic, message);
					} catch (Exception e) {
						logger.error( "==Exception Occur !!! - consuming topic = " + topic
							 + ", streamNum = " + streamNum + ", messageBytes = " + message.length(), e);
					}
				}
			} catch (Exception e) {
				logger.error("=[" + threadName + "] Exception Occur!!!", e);
			} finally {
				logger.error("=[" + threadName + "] stopped!!!");
			}
		}
	}


}
