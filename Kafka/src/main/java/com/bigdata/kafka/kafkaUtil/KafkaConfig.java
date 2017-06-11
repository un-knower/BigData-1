package com.bigdata.kafka.kafkaUtil;

import com.bigdata.kafka.util.ConfigFactory;

import java.util.Properties;

/**
 * 
 * @ClassName: KafkaConfig 
 * @Description: kafka配置信息  
 * @author taocl@jusfoun.com
 * @date 2016年3月24日
 *
 */
public class KafkaConfig {
 
	private static Properties producerPro ; //生产者
	private static Properties consumerPro ; //消费者属性文件
	
 
	//初始化生产者 属性文件 
	public static Properties getProducerPro(){
		if(producerPro!=null){
			return clonePro(producerPro) ; 
		}
		//metadata.broker.list accepts input in the form "host1:port1,host2:port2"
		producerPro = new Properties();
		producerPro.put("broker.list", ConfigFactory.getString( "kafka.brokerList") );
		producerPro.put("metadata.broker.list",  ConfigFactory.getString("kafka.metadataBrokerList")  );
		//必须实现kafka.serializer.Encoder接口，将T类型的对象encode成kafka message
		producerPro.put("serializer.class",  ConfigFactory.getString("kafka.serializerClass") );
		//producerPro.put("partitioner.class", partioner);
		//async 异步发送   sync 同步发送 
		producerPro.put("producer.type",  ConfigFactory.getString("kafka.producerType")  );
		//	0表示producer毋须等待leader的确认，1代表需要leader确认写入它的本地log并立即确认，-1代表所有的备份都完成后确认。 仅仅for sync
		producerPro.put("request.required.acks",  ConfigFactory.getString("kafka.requestRequiredAcks") );
		producerPro.put("message.send.max.retries",  ConfigFactory.getString("kafka.messageSendMaxRetries")  ); // default=3
		//	一批消息的数量，仅仅for asyc （异步）
		producerPro.put("batch.num.messages",  ConfigFactory.getString("kafka.batchNumMessages"));
		producerPro.put("send.buffer.bytes",  ConfigFactory.getString("kafka.sendBufferBytes") ); //Socket write buffer size
		producerPro.put("request.timeout.ms",  ConfigFactory.getString("kafka.requestTimeoutMs") ); //	确认超时时间
		producerPro.put("zk.connect",  ConfigFactory.getString( "kafka.zkConnect"));  //声明zk
		//缓存数据的最大时间
		producerPro.put("queue.buffering.max.ms",  ConfigFactory.getString("kafka.queueBufferingMaxMs"));
		//被缓存的数据的上限
		producerPro.put("queue.buffering.max.messages",  ConfigFactory.getString("kafka.queueBufferingMaxMessages") ); //Socket write buffer size
		//数据超过上限是否block
		producerPro.put("queue.enqueue.timeout.ms",  ConfigFactory.getString("kafka.queueEnqueueTimeoutMs") ); //	确认超时时间
		//数据压缩方式
		producerPro.put("compression.codec",  ConfigFactory.getString("kafka.compressionCodec") ); //	确认超时时间
		
		
		//props.put("queue.enqueue.timeout.ms", "-1"); // 		0当queue满时丢掉，负值是queue满时block,正值是queue满时block相应的时间，仅仅for asyc 
		//props.put("queue.buffering.max.message", "-1"); // 		producer 缓存的消息的最大数量，仅仅for asyc
		//props.put("queue.buffering.max.ms", "-1"); //  在producer queue的缓存的数据最大时间，仅仅for asyc
		
		//props.put("compression.codec", "1");

//		producerPro = new Properties();
//		producerPro.put("broker.list", "ubt202:6667");				
//		producerPro.put("metadata.broker.list",  "ubt202:6667" );
//		//必须实现kafka.serializer.Encoder接口，将T类型的对象encode成kafka message
//		producerPro.put("serializer.class",  "kafka.serializer.StringEncoder");
//		//producerPro.put("partitioner.class", partioner);
//		//async 异步发送   sync 同步发送 
//		producerPro.put("producer.type", "sync");
//		//	0表示producer毋须等待leader的确认，1代表需要leader确认写入它的本地log并立即确认，-1代表所有的备份都完成后确认。 仅仅for sync
//		producerPro.put("request.required.acks","1");
//		producerPro.put("message.send.max.retries", "3" ); // default=3
//		//	一批消息的数量，仅仅for asyc
//		producerPro.put("batch.num.messages","4000");
//		producerPro.put("send.buffer.bytes", "1048576" ); //Socket write buffer size
//		producerPro.put("request.timeout.ms", "8000" ); //	确认超时时间
//		producerPro.put("zk.connect", "ubt202:2181,ubt204:2181,ubt203:2181");  //声明zk
		return clonePro(producerPro) ; 
	}

 
	//初始化消费者 属性文件 
	public static Properties getConsumerPro(){
		if(consumerPro!=null){
			return clonePro(consumerPro) ;  
		}
		consumerPro = new Properties();
        //zookeeper 配置
        consumerPro.put("zookeeper.connect", ConfigFactory.getString( "kafka.zkConnect"));
        //group 代表一个消费组
        consumerPro.put("group.id", ConfigFactory.getString("kafka.groupId"));
        //zk连接超时时间
        consumerPro.put("zookeeper.session.timeout.ms", ConfigFactory.getString("kafka.zookeeperSessionTimeoutMs") );
        consumerPro.put("zookeeper.sync.time.ms",  ConfigFactory.getString("kafka.zookeeperSyncTimeMs") );
        //offsets 被提交到 zookeeper的频率.
        consumerPro.put("auto.commit.interval.ms",  ConfigFactory.getString("kafka.autoCommitIntervalMs") );
        //当没有初始化offset时候，用可以获得的最小范围
        consumerPro.put("auto.offset.reset",  ConfigFactory.getString("kafka.autoOffsetReset"));
        //序列化类
        consumerPro.put("serializer.class",  ConfigFactory.getString("kafka.serializerClass"));	
        return clonePro(consumerPro) ;  
	}
	
	//克隆一个属性文件
	private static Properties clonePro(Properties pro){
		Properties ptmp = new Properties();
		ptmp.putAll( pro );
		return ptmp; 
	}
}
