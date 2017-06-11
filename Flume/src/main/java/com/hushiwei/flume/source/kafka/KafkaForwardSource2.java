package com.hushiwei.flume.source.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */

import com.hushiwei.flume.utils.Utils;
import com.hushiwei.kafka.KafkaConsumerConfig;
import com.hushiwei.kafka.KafkaConsumerManager;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.util.*;

//import java.util.List<Lorg.apache.flume.Event;>;

public class KafkaForwardSource2 extends AbstractSource
        implements Configurable, PollableSource {
    private static Logger logger = Logger.getLogger(KafkaForwardSource2.class);
    private ConsumerConnector consumer;
    private ConsumerIterator<byte[], byte[]> it;
    private String topic;
    private int batchUpperLimit;
    private int timeUpperLimit;
    private int consumerTimeout;
    private boolean kafkaAutoCommitEnabled;
    private Context context;
    private Properties kafkaProps;
    private final List<Event> eventList = new ArrayList();
    private KafkaSourceCounter counter;
    private long firstCount = 0L;
    private long lastCount = 0L;
    private boolean stop;
    private boolean stopDone;

    public PollableSource.Status process()
            throws EventDeliveryException {
        long batchStartTime = System.currentTimeMillis();
        long batchEndTime = System.currentTimeMillis() + this.timeUpperLimit;
        try {
            boolean iterStatus = false;
            long startTime = System.nanoTime();
            while ((this.eventList.size() < this.batchUpperLimit) && (
                    System.currentTimeMillis() < batchEndTime)) {
                iterStatus = hasNext();
                if (iterStatus) {
                    this.firstCount += 1L;
                    this.counter.incrementAppendReceivedCount();

                    MessageAndMetadata messageAndMetadata = this.it.next();

                    List events = processKafakMessage(messageAndMetadata);
                    if (events.size() > 0) {
                        this.eventList.addAll(events);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Waited:  " + (System.currentTimeMillis() - batchStartTime));
                    logger.debug("Event #: " + this.eventList.size());
                }
            }
            long endTime = System.nanoTime();
            this.counter.addToKafkaEventGetTimer((endTime - startTime) / 1000000L);
            this.counter.addToEventReceivedCount(Long.valueOf(this.eventList.size()).longValue());

            if (this.eventList.size() > 0) {
                getChannelProcessor().processEventBatch(this.eventList);
                this.counter.addToEventAcceptedCount(this.eventList.size());
                this.eventList.clear();
                if (logger.isDebugEnabled()) {
                    logger.debug("Wrote " + this.eventList.size() + " events to channel");
                }
                if (!this.kafkaAutoCommitEnabled) {
                    long commitStartTime = System.nanoTime();
                    this.consumer.commitOffsets();
                    long commitEndTime = System.nanoTime();
                    this.counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / 1000000L);
                }
            }
            if (!iterStatus) {
                if (logger.isDebugEnabled()) {
                    this.counter.incrementKafkaEmptyCount();
                    logger.debug("Returning with backoff. No more data to read");
                }
                return PollableSource.Status.BACKOFF;
            }
            if (this.stop) {
                this.stopDone = true;
            }
            return PollableSource.Status.READY;
        } catch (Exception e) {
            logger.error("KafkaSource EXCEPTION, " + e.getMessage(), e);
            if (this.stop)
                this.stopDone = true;
        }
        return PollableSource.Status.BACKOFF;
    }

    public static List<Event> processKafakMessage(MessageAndMetadata<byte[], byte[]> mm) {
        List events = new ArrayList();
        try {
            if ((mm == null) || (mm.message() == null) || (((byte[]) mm.message()).length == 0)) {
                return events;
            }

            String message = new String((byte[]) mm.message(), "UTF-8");

            String[] fields = message.split("\\s+");
            if (fields.length != 5) {
                logger.error("报文格式不对:" + message);
                return events;
            }

            String VIN = fields[2];
            String infoType = fields[3];
            String info = fields[4];

            Map<String, String> infoMap = Utils.processMessageToMap(info);

            StringBuffer buf = new StringBuffer();
            String VID = (String) infoMap.remove("VID");
            String TIME = (String) infoMap.remove("TIME");
            if (TIME == null) {
                TIME = (String) infoMap.get("2000");
            }
            buf.append("VID:").append(VID).append(",");
            buf.append("VIN:").append(VIN).append(",");
            buf.append("TIME:").append(TIME).append(",");
            buf.append("MESSAGETYPE:FORWARD");

            for (Map.Entry entry : infoMap.entrySet()) {
                buf.append(",").append((String) entry.getKey()).append(":").append((String) entry.getValue());
            }
            Event hdfsEvent = new SimpleEvent();
            Object hdfsHeader = new HashMap();

            ((Map) hdfsHeader).put("CHANNEL_KEY", "HDFS");
            ((Map) hdfsHeader).put("category", "forward/" + Utils.getTimePath(TIME));
            hdfsEvent.setHeaders((Map) hdfsHeader);
            byte[] body = buf.toString().getBytes("UTF-8");
            hdfsEvent.setBody(body);
            events.add(hdfsEvent);

            Event hbaseEvent = new SimpleEvent();
            Map hbaseHeader = new HashMap();
            hbaseHeader.put("CHANNEL_KEY", "FORWARD");
            hbaseEvent.setHeaders(hbaseHeader);
            hbaseEvent.setBody(body);

            events.add(hbaseEvent);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取解析kafka消息失败:" + e.getMessage());
            logger.error("获取解析kafka消息失败:", e);
        }
        return (List<Event>) events;
    }

    public void configure(Context context) {
        this.context = context;
        this.batchUpperLimit = context.getInteger("batchSize",
                Integer.valueOf(1000)).intValue();

        this.timeUpperLimit = context.getInteger("batchDurationMillis",
                Integer.valueOf(1000)).intValue();

        this.topic = context.getString("topic");

        if (this.topic == null) {
            throw new ConfigurationException("Kafka topic must be specified.");
        }

        this.kafkaProps = KafkaSourceUtil.getKafkaProperties(context);
        this.consumerTimeout = Integer.parseInt(this.kafkaProps.getProperty(
                "consumer.timeout.ms"));
        this.kafkaAutoCommitEnabled = Boolean.parseBoolean(this.kafkaProps.getProperty(
                "auto.commit.enable"));

        if (this.counter == null)
            this.counter = new KafkaSourceCounter(getName());
    }

    public synchronized void start() {
        logger.info("Starting " + this + "...");
        try {
            this.consumer = KafkaSourceUtil.getConsumer(this.kafkaProps);
        } catch (Exception e) {
            throw new FlumeException("Unable to create consumer. Check whether the ZooKeeper server is up and that the Flume agent can connect to it.",
                    e);
        }

        Map topicCountMap = new HashMap();

        topicCountMap.put(this.topic, Integer.valueOf(1));
        try {
            Map consumerMap =
                    this.consumer.createMessageStreams(topicCountMap);
            List topicList = (List) consumerMap.get(this.topic);
            KafkaStream stream = (KafkaStream) topicList.get(0);
            this.it = stream.iterator();
        } catch (Exception e) {
            throw new FlumeException("Unable to get message iterator from Kafka", e);
        }
        logger.info("Kafka source " + getName() + " started.");
        this.counter.start();
        super.start();

        new Thread() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    KafkaForwardSource2.logger.info("forward topic 5秒消费计数:--------------" + (KafkaForwardSource2.this.firstCount - KafkaForwardSource2.this.lastCount));
                    KafkaForwardSource2.this.lastCount = KafkaForwardSource2.this.firstCount;
                }
            }
        }
                .start();
    }

    public synchronized void stop() {
        if (this.consumer != null) {
            this.consumer.shutdown();
        }
        this.stop = true;
        long endTime = System.currentTimeMillis() + 3000L;

        while ((!this.stopDone) && (System.currentTimeMillis() < endTime)) ;
        logger.info("forward  process 处理完毕，forward source 关闭");

        this.counter.stop();
        logger.info("Kafka Source " + getName() + " stopped. Metrics: " + this.counter);
        super.stop();
    }

    boolean hasNext() {
        try {
            this.it.hasNext();
            return true;
        } catch (ConsumerTimeoutException e) {
        }
        return false;
    }

    public static void main(String[] args) {
        ConsumerConfig config = KafkaConsumerConfig.createConfig("10.8.6.161", "g_forward");
        KafkaConsumerManager consumerManager = new KafkaConsumerManager(config, "test_forward", 1);
        List<KafkaStream<byte[], byte[]>> list = consumerManager.createKafkaStreams();

        ConsumerIterator it = null;
        for (KafkaStream kafkaStream : list) {
            it = kafkaStream.iterator();
        }
        while (it.hasNext())
            processKafakMessage(it.next());
    }
}
