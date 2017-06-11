package com.hushiwei.flume.source.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.hushiwei.flume.utils.Utils;
import com.hushiwei.kafka.KafkaConsumerConfig;
import com.hushiwei.kafka.KafkaConsumerManager;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

public class KafkaForwardSource extends AbstractSource
        implements Configurable, PollableSource
{
    private static Logger logger = Logger.getLogger(KafkaForwardSource.class);
    private static LinkedBlockingQueue<Event> que = new LinkedBlockingQueue(100);
    private ExecutorService executor = null;
    private List<KafkaReader> readers = new ArrayList();
    private KafkaConsumerManager consumerManager;
    private SourceCounter counter;
    private String zk;
    private String groupName;
    private String topic;
    private int numStreams;

    public PollableSource.Status process()
            throws EventDeliveryException
    {
        Event event = (Event)que.poll();
        if (event == null) {
            return PollableSource.Status.BACKOFF;
        }

        this.counter.addToEventReceivedCount(1L);
        getChannelProcessor().processEvent(event);

        this.counter.addToEventAcceptedCount(1L);
        return PollableSource.Status.READY;
    }

    public void configure(Context context)
    {
        logger.info("kafka sources初始化配置文件");

        this.zk = context.getString("zk");
        this.groupName = context.getString("groupName");
        this.topic = context.getString("topic");
        this.numStreams = context.getInteger("numStreams").intValue();
    }

    public synchronized void start()
    {
        logger.info("KafkaForwardSource开始启动");
        this.counter = new SourceCounter(getName());
        this.counter.start();
        ConsumerConfig config = KafkaConsumerConfig.createConfig(this.zk, this.groupName);
        this.consumerManager = new KafkaConsumerManager(config, this.topic, this.numStreams);
        List<KafkaStream<byte[], byte[]>> list = this.consumerManager.createKafkaStreams();

        this.executor = Executors.newFixedThreadPool(this.numStreams);
        for (KafkaStream kafkaStream : list) {
            KafkaReader reader = new KafkaReader(kafkaStream, que);
            this.readers.add(reader);
            this.executor.submit(reader);
        }
        logger.info("kafka sources启动完毕");
        super.start();
    }

    public synchronized void stop()
    {
        this.consumerManager.getJavaConsumerConnector().commitOffsets();

        this.consumerManager.getJavaConsumerConnector().shutdown();

        this.executor.shutdown();

        this.readers.clear();
        this.counter.stop();
        super.stop();
    }

    private static class KafkaReader
            implements Runnable
    {
        private KafkaStream<byte[], byte[]> stream;
        private LinkedBlockingQueue<Event> que;

        public KafkaReader(KafkaStream<byte[], byte[]> stream, LinkedBlockingQueue<Event> que)
        {
            this.stream = stream;
            this.que = que;
        }

        public void run() {
            ConsumerIterator it = this.stream.iterator();
            while (it.hasNext())
                try
                {
                    MessageAndMetadata mm = it.makeNext();
                    if (mm == null)
                    {
                        break;
                    }
                    String message = new String((byte[])mm.message(), "UTF-8");

                    String[] fields = message.split("\\s+");
                    if (fields.length != 5) {
                        KafkaForwardSource.logger.error("报文格式不对:" + message);
                        return;
                    }

                    String VIN = fields[2];
                    String infoType = fields[3];
                    String info = fields[4];

                    Map<String, String> infoMap = Utils.processMessageToMap(info);

                    StringBuffer buf = new StringBuffer();
                    String VID = (String)infoMap.remove("VID");
                    String TIME = (String)infoMap.remove("TIME");
                    buf.append("VID:").append(VID).append(",");
                    buf.append("VIN:").append(VIN).append(",");
                    buf.append("TIME:").append(TIME).append(",");
                    buf.append("MESSAGETYPE:FORWARD");

                    for (Map.Entry entry : infoMap.entrySet()) {
                        buf.append(",").append((String)entry.getKey()).append(":").append((String)entry.getValue());
                    }
                    Event hdfsEvent = new SimpleEvent();
                    Object hdfsHeader = new HashMap();

                    ((Map)hdfsHeader).put("CHANNEL_KEY", "HDFS");
                    ((Map)hdfsHeader).put("category", "forward/" + Utils.getTimePath(TIME));
                    hdfsEvent.setHeaders((Map)hdfsHeader);
                    byte[] body = buf.toString().getBytes("UTF-8");
                    hdfsEvent.setBody(body);

                    this.que.put(hdfsEvent);

                    Event hbaseEvent = new SimpleEvent();
                    Map hbaseHeader = new HashMap();
                    hbaseHeader.put("CHANNEL_KEY", "FORWARD");
                    hbaseEvent.setHeaders(hbaseHeader);
                    hbaseEvent.setBody(body);
                    this.que.put(hbaseEvent);
                } catch (Exception e) {
                    e.printStackTrace();
                    KafkaForwardSource.logger.error("获取解析kafka消息失败:" + e.getMessage());
                    KafkaForwardSource.logger.error("获取解析kafka消息失败:", e);
                }
        }
    }
}
