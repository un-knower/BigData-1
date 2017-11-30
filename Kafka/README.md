
## kafka相关命令说明
0. 在每台都启动kafka
```
./bin/kafka-server-start.sh config/server.properties >/opt/logs/kafka-serve &
```

1. 创建一个topic 分区数和复本数都是1
```
 ./bin/kafka-topics.sh --create --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --replication-factor 1 --partitions 1 --topic flume-kafka
```

2. 查看这个topic的信息
```
 ./bin/kafka-topics.sh --describe --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --topic flume-kafka
```

3. 查询所有可用的topic
```
./bin/kafka-topics.sh --list --zookeeper ncp163:2181,ncp161:2181,ncp162:2181
```

4. 修改topic 把分区数修改为3
```
./bin/kafka-topics.sh --alter --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --partition 3 --topic flume-kafka
```

5. 删除topic
```
bin/kafka-topics.sh --delete --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --topic flume-kafka
```
注意:HI-Jusfoun只是被标记为删除了。kafka为了数据的安全，不允许随便删除数据。如果要完全删除，需要修改配置
   vi config/server.propertiex 在这个文件的最后加上这句 delete.topic.enable=true
   然后，停止kafka,再启动kafka服务。

6. 创建生产者 localhost:9092这个根据情况要修改
 ```
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic flume-kafka

 ```
7. 创建消费者
```
./bin/kafka-console-consumer.sh --zookeeper ncp163:2181,ncp161:2181,ncp162:2181 --topic flume-kafka --from-beginning
```
