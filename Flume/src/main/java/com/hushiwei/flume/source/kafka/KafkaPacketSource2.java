package com.hushiwei.flume.source.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.hushiwei.flume.exception.InvalidMessageException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

public class KafkaPacketSource2 extends AbstractSource
        implements Configurable, PollableSource {
    private static Logger logger = Logger.getLogger(KafkaPacketSource2.class);
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

                    Event event = processKafakMessage(messageAndMetadata);
                    if (event != null) {
                        this.eventList.add(event);
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

    private Event processKafakMessage(MessageAndMetadata<byte[], byte[]> mm) {
        try {
            if ((mm == null) || (mm.message() == null) || (((byte[]) mm.message()).length == 0)) {
                return null;
            }

            boolean hasVid = hasVid(new String((byte[]) mm.message(), "UTF-8"));

            Map header = new HashMap();
            if (hasVid) {
                header.put("CHANNEL_KEY", "PACKET");
            } else {
                header.put("CHANNEL_KEY", "PACKET_ERROR");
            }
            Event event = EventBuilder.withBody((byte[]) mm.message(), header);
            return event;
        } catch (InvalidMessageException me) {
            me.printStackTrace();
            logger.error("获取kafka消息失败:" + me.getMessage(), me);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取kafka消息失败:" + e.getMessage());
            logger.error("获取kafka消息失败:", e);
        }
        return null;
    }

    private static boolean hasVid(String message) throws InvalidMessageException {
        boolean result = true;
        String[] fields = message.split("\\s+");
        if (fields.length != 5) {
            logger.error("数据格式错误:" + message);
            throw new InvalidMessageException("数据格式错误:" + message);
        }
        if ((!fields[3].equalsIgnoreCase("PACKET")) ||
                (!fields[0].equalsIgnoreCase("SUBMIT"))) {
            logger.error("报文类型不对,数据丢弃:" + message);
            throw new InvalidMessageException("报文类型不对,数据丢弃:" + message);
        }

        String json = fields[4];

        String[] jsonFields = json.substring(1, json.length() - 1)
                .split(",");

        for (String keyValue : jsonFields) {
            String[] kv = keyValue.split(":");
            if ((kv.length == 2) && (kv[0].equals("VID"))) {
                return true;
            }
        }
        return result;
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
                    KafkaPacketSource2.logger.info("packet topic 5秒消费计数:--------------" + (KafkaPacketSource2.this.firstCount - KafkaPacketSource2.this.lastCount));
                    KafkaPacketSource2.this.lastCount = KafkaPacketSource2.this.firstCount;
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
        logger.info("packet  process 处理完毕，packet source 关闭");

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
}
