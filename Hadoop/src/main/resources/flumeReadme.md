##log4j+flume+kafka+sparkStreaming整合步骤

###整合log4j和flume
1：找到爬虫项目，写一个测试类，自己产生日志(见Creator.java)
2：部署flume，修改flume的配置
	vi conf/flume-conf.properties
	agent1.channels = ch1
	agent1.sources = avro-source1
	agent1.sinks = log-sink1

	# 定义channel
	agent1.channels.ch1.type = memory

	# 定义source
	agent1.sources.avro-source1.channels = ch1
	agent1.sources.avro-source1.type = avro
	agent1.sources.avro-source1.bind = 0.0.0.0
	agent1.sources.avro-source1.port = 41414

	# 定义sink
	agent1.sinks.log-sink1.channel = ch1
	agent1.sinks.log-sink1.type = logger

	启动flume
	bin/flume-ng agent --conf conf --conf-file conf/flume-conf.properties --name agent1 -Dflume.root.logger=INFO,console


3：修改爬虫项目中的log4j.properties
	log4j.rootLogger=info,flume



	log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender
	log4j.appender.flume.Hostname = 192.168.16.165
	log4j.appender.flume.Port = 41414
	log4j.appender.flume.UnsafeMode = true
	log4j.appender.flume.layout=org.apache.log4j.PatternLayout
	log4j.appender.flume.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} [%t] [%c] [%p] - %m%n

4：执行测试类，发现启动flume的控制台的日志一直滚动即可。
	这样就说明log4j和flume整合成功了。


整合flume和kafka
5：部署kafka，并且要验证kafka可以正常是使用
	创建一个主题
	bin/kafka-topics.sh --create --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --replication-factor 1 --partitions 1 --topic flume-kafka

	写一个生产者，再启动一个kafka的消费者，验证kafka是否正常
	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("metadata.broker.list", "192.168.1.170:9092");
		prop.put("serializer.class", StringEncoder.class.getName());
		ProducerConfig producerConfig = new ProducerConfig(prop );
		Producer<String, String> producer = new Producer<String,String>(producerConfig);
		KeyedMessage<String, String> message = new KeyedMessage<String, String>("spider", "www.crxy.cn");
		producer.send(message);
	}

	启动kafka的消费者
	bin/kafka-console-consumer.sh --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --topic flume-kafka --from-beginning

	验证通告之后修改flume的sink配置
	vi flume/conf/flume-conf.properties

	agent1.channels = ch1
	agent1.sources = avro-source1
	agent1.sinks = log-sink1

	# 定义channel
	agent1.channels.ch1.type = memory

	# 定义source
	agent1.sources.avro-source1.channels = ch1
	agent1.sources.avro-source1.type = avro
	agent1.sources.avro-source1.bind = 0.0.0.0
	agent1.sources.avro-source1.port = 41414

	# 定义sink
	agent1.sinks.log-sink1.channel = ch1
	agent1.sinks.log-sink1.type = org.apache.flume.sink.kafka.KafkaSink
	agent1.sinks.log-sink1.topic = flume-kafka
	agent1.sinks.log-sink1.brokerList = 192.168.16.165:6667
	agent1.sinks.log-sink1.requiredAcks = 1
	agent1.sinks.log-sink1.batchSize = 1

	再启动flume

	在log4j的测试类中产生日志，在kafka的消费者端查看是否可以收到日志，如果能收到，说明log4j+flume+kafka整合成功



kafka和storm整合
6：先创建一个storm的项目，把项目整体框架写出来，主要是spout这一块的代码
	需要用到storm-kafka项目中的一个类，kafkaspout，在创建kafkaspout的时候需要指定一些配置信息，具体如下
		BrokerHosts hosts = new ZkHosts("192.168.16.161:2181,192.168.16.162:2181,192.168.16.163:2181");//表示kafka的地址
		String topic = "flume-kafka";//主题的名称
		String zkRoot = "/kafkaspout";//这是一个根节点，在这个节点下面保存kafka中数据的读取位置等信息
		String id = "2016";//相当于是groupid
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		new KafkaSpout(spoutConf);

	注意：kafkaspout开启了acker消息确认机制，并且在fail方法中实现了数据重新发送的功能。














