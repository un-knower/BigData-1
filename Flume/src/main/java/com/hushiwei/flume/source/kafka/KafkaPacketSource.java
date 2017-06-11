/*
package com.hushiwei.flume.source.kafka;

*/
/**
 * Created by HuShiwei on 2016/8/21 0021.
 *//*

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.hushiwei.flume.exception.InvalidMessageException;
import com.hushiwei.kafka.KafkaConsumerConfig;
import com.hushiwei.kafka.KafkaConsumerManager;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

public class KafkaPacketSource extends AbstractSource
        implements Configurable, PollableSource
{
    private static Logger logger = Logger.getLogger(KafkaPacketSource.class);
    private static LinkedBlockingQueue<Event> que;
    private ExecutorService executor = null;
    private List<KafkaReader> readers = new ArrayList();
    private KafkaConsumerManager consumerManager;
    private SourceCounter counter;
    private String zk;
    private String groupName;
    private String topic;
    private int numStreams;
    private int batchSize = 300;
    private int queSize = 10000;
    private long kc = 0L;
    private long last = 0L;
    boolean stop = false;
    private List<Event> list;
    private AtomicInteger start = new AtomicInteger(0);

    public PollableSource.Status process() throws EventDeliveryException
    {
        while (true)
            try
            {
                Event event = (Event)que.poll();
                if (event != null) continue;
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;

                if (this.list == null) {
                    this.list = new ArrayList<Event>();
                }
                this.list.add(event);
                this.kc += 1L;
                if (this.kc % 1000L == 0L) {
                    logger.info("原始报文数据大小:" + event.getBody().length);
                }
                if (this.list.size() < this.batchSize)
                    continue;
                this.counter.addToEventReceivedCount(this.list.size());
                getChannelProcessor().processEventBatch(this.list);
                logger.info("原始报文到达" + this.batchSize + "已经批量提交到channel" + "队列数据:" + que.size());

                this.counter.addToEventAcceptedCount(this.list.size());
                this.list = null;
                if (this.stop) {
                    return PollableSource.Status.READY;

                    if ((!this.stop) || (que.size() != 0) ||
                            (this.list.size() <= 0)) continue;
                    getChannelProcessor().processEventBatch(this.list);
                    return PollableSource.Status.READY;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
    }

    public void configure(Context context)
    {
        logger.info("kafka sources初始化配置文件");

        this.zk = context.getString("zk");
        this.groupName = context.getString("groupName");
        this.topic = context.getString("topic");
        this.numStreams = context.getInteger("numStreams").intValue();
        this.batchSize = context.getInteger("batchSize", Integer.valueOf(300)).intValue();
        this.queSize = context.getInteger("queSize", Integer.valueOf(10000)).intValue();

        que = new LinkedBlockingQueue(this.queSize);

        new Thread()
        {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    KafkaPacketSource.logger.info("packet topic 5秒消费计数:--------------" + (KafkaPacketSource.this.kc - KafkaPacketSource.this.last));
                    KafkaPacketSource.this.last = KafkaPacketSource.this.kc;
                }
            }
        }
                .start();
    }

    public synchronized void start()
    {
        logger.info("KafkaPacketSource开始启动");
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
        this.stop = true;
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
                    boolean hasVid = hasVid(new String((byte[])mm.message(), "UTF-8"));
                    Map header = new HashMap();
                    if (hasVid)
                    {
                        header.put("CHANNEL_KEY", "PACKET");
                    }
                    else {
                        header.put("CHANNEL_KEY", "PACKET_ERROR");
                    }
                    Event event = EventBuilder.withBody((byte[])mm.message(), header);
                    this.que.put(event);
                } catch (InvalidMessageException me) {
                    me.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                    KafkaPacketSource.logger.error("获取kafka消息失败:" + e.getMessage());
                    KafkaPacketSource.logger.error("获取kafka消息失败:", e);
                }
        }

        private boolean hasVid(String message) throws InvalidMessageException
        {
            boolean result = true;
            String[] fields = message.split("\\s+");
            if (fields.length != 5) {
                KafkaPacketSource.logger.error("数据格式错误:" + message);
                throw new InvalidMessageException("数据格式错误:" + message);
            }
            if ((!fields[3].equalsIgnoreCase("PACKET")) ||
                    (!fields[0].equalsIgnoreCase("SUBMIT")))
            {
                KafkaPacketSource.logger.error("报文类型不对,数据丢弃:" + message);
                throw new InvalidMessageException("报文类型不对,数据丢弃:" + message);
            }

            String json = fields[4];

            String[] jsonFields = json.substring(1, json.length() - 1)
                    .split(",");

            Map jsonMap = new HashMap();
            for (String keyValue : jsonFields) {
                String[] kv = keyValue.split(":");
                jsonMap.put(kv[0], kv[1]);
            }
            String VID = StringUtils.trimToEmpty((String)jsonMap.get("VID"));
            if (VID.equals("")) {
                result = false;
            }
            return result;
        }
    }
}
*/
