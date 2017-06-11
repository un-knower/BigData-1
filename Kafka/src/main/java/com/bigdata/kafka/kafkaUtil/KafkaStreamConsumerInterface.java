package com.bigdata.kafka.kafkaUtil;

/**
 * 
 * @ClassName: KafkaStreamConsumerInterface 
 * @Description:  kafka 消费 回调接口
 * @author taocl@jusfoun.com
 * @date 2016年3月24日
 */
public interface KafkaStreamConsumerInterface {
	/**
	 * 
	 * @param topic   topic 名称 
	 * @param message 收到的消息信息
	 */
	public void consume(String topic, String message);
}
