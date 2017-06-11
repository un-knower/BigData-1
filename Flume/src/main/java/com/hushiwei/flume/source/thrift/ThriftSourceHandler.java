package com.hushiwei.flume.source.thrift;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hushiwei.flume.utils.CompressUtil;
import com.hushiwei.flume.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.thrift.Status;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.flume.thrift.ThriftSourceProtocol.Iface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class ThriftSourceHandler
        implements ThriftSourceProtocol.Iface, Runnable {
    private static final Logger logger = Logger.getLogger(ThriftSourceHandler.class);
    private ChannelProcessor processor;
    private SourceCounter sourceCounter;
    private LinkedBlockingQueue<ThriftFlumeEvent> que = new LinkedBlockingQueue(10000);
    private long tc = 0L;
    private long last = 0L;
    private AtomicBoolean stop;
    private int batchUpperLimit = 1000;
    private int timeUpperLimit = 1000;

    public Status append(ThriftFlumeEvent event2) throws TException {
        try {
            if (event2 != null) {
                this.que.put(event2);
                try {
                    logger.info("append 收到数据:" + new String(event2.getBody(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        return Status.OK;
    }

    private Event createHdfsEvent(Map<String, String> keyValues) {
        try {
            String VID = StringUtils.trimToEmpty((String) keyValues.remove("VID"));
            String VIN = StringUtils.trimToEmpty((String) keyValues.remove("VIN"));
            String TIME = StringUtils.trimToEmpty((String) keyValues.remove("TIME"));
            String MESSAGETYPE = StringUtils.trimToEmpty((String) keyValues.remove("MESSAGETYPE"));

            StringBuffer buf = new StringBuffer();
            buf.append("VID:" + VID).append(",").append("VIN:" + VIN)
                    .append(",").append("TIME:" + TIME).append(",")
                    .append("MESSAGETYPE:" + MESSAGETYPE).append(",");

            for (Map.Entry entry : keyValues.entrySet()) {
                buf.append((String) entry.getKey()).append(":").append((String) entry.getValue())
                        .append(",");
            }
            buf.deleteCharAt(buf.length() - 1);

            Event hdfsEvent = new SimpleEvent();
            Object hdfsHeader = new HashMap();

            ((Map) hdfsHeader).put("CHANNEL_KEY", "HDFS");

            if (MESSAGETYPE.equals("ALARM")) {
                ((Map) hdfsHeader).put("category", "alarm/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("REALTIME")) {
                ((Map) hdfsHeader).put("category", "realinfo/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("RENTCAR")) {
                ((Map) hdfsHeader).put("category", "rentcar/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("FORWARD")) {
                ((Map) hdfsHeader).put("category", "forward/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("LOGIN")) {
                ((Map) hdfsHeader).put("category", "login/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("HISTORY")) {
                ((Map) hdfsHeader).put("category", "realinfo/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("TERMSTATUS")) {
                ((Map) hdfsHeader).put("category", "termstatus/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("CARSTATUS")) {
                ((Map) hdfsHeader).put("category", "carstatus/" + Utils.getTimePath(TIME));
            } else if (MESSAGETYPE.equals("CHARGE")) {
                ((Map) hdfsHeader).put("category", "charge/" + Utils.getTimePath(TIME));
            } else {
                logger.error("MESSAGETYPE不正确:" + MESSAGETYPE);
                return null;
            }
            hdfsEvent.setHeaders((Map) hdfsHeader);
            hdfsEvent.setBody(Bytes.toBytes(buf.toString()));

            return hdfsEvent;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("处理消息失败:" + e.getMessage());
            logger.error("处理消息失败:", e);
        }
        return (Event) null;
    }

    public ThriftSourceHandler(ChannelProcessor processor, SourceCounter sourceCounter, AtomicBoolean stop) {
        this.processor = processor;
        this.sourceCounter = sourceCounter;
        this.stop = stop;

        new Thread() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ThriftSourceHandler.logger.info("realinfo  5秒接收计数:--------------" + (ThriftSourceHandler.this.tc - ThriftSourceHandler.this.last) + ",队列长度:" + ThriftSourceHandler.this.que.size());
                    ThriftSourceHandler.this.last = ThriftSourceHandler.this.tc;
                }
            }
        }
                .start();

        new Thread(this).start();
    }

    public Status appendBatch(List<ThriftFlumeEvent> events)
            throws TException {
        List eventList = new ArrayList();
        for (ThriftFlumeEvent event : events) {
            List tempList = processMessage(event);
            eventList.addAll(tempList);
        }
        this.processor.processEventBatch(eventList);
        this.sourceCounter.addToEventReceivedCount(eventList.size());

        return Status.OK;
    }

    public void run() {
        List eventList = new ArrayList();
        while (true)
            try {
                long batchEndTime = System.currentTimeMillis() + this.timeUpperLimit;
//                continue;

                ThriftFlumeEvent event = (ThriftFlumeEvent) this.que.poll();
                if (event != null) continue;
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                this.tc += 1L;
                List tempEventList = processMessage(event);
                if (tempEventList.size() > 0)
                    eventList.addAll(tempEventList);
                if ((eventList.size() < this.batchUpperLimit) && (
                        System.currentTimeMillis() < batchEndTime)) {
                    continue;
                }

                if (eventList.size() > 0) {
                    this.sourceCounter.addToEventReceivedCount(eventList.size());
                    this.processor.processEventBatch(eventList);

                    eventList.clear();
                }

                if (!this.stop.get())
                    continue;
                if (this.que.size() > 0) {
                    ThriftFlumeEvent event1 = (ThriftFlumeEvent) this.que.poll();
                    List<Event> tempEventList1 = processMessage(event);
                    if (tempEventList.size() > 0) {
                        this.processor.processEventBatch(tempEventList1);

                        continue;
                    }
                }
                return;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("hendler处理线程的队列出错:" + e.getMessage(), e);
                logger.error(e.getStackTrace());
            }
    }

    private List<Event> processMessage(ThriftFlumeEvent event) {
        List eventList = new ArrayList();
        try {
            String eventType = StringUtils.trimToEmpty((String) event.getHeaders().get("DATATYPE"));

            String compress = StringUtils.trimToEmpty((String) event.getHeaders().get("COMPRESS"));

            String message = null;
            if (compress.equalsIgnoreCase("true")) {
                message = CompressUtil.uncompress(event.getBody());
            } else {
                message = new String(event.getBody(), "UTF-8");
            }
            Map keyValues = Utils.processMessageToMap(message);

            String MESSAGETYPE = StringUtils.trimToEmpty((String) keyValues.get("MESSAGETYPE"));

            if (eventType.equals("ALARM")) {
                MESSAGETYPE = "ALARM";
                keyValues.put("MESSAGETYPE", "ALARM");
            }
            String str1;
            switch ((str1 = MESSAGETYPE).hashCode()) {
                case -76571285:
                    if (str1.equals("REALTIME")) ;
                    break;
                case 40836773:
                    if (str1.equals("FORWARD")) ;
                    break;
                case 62358065:
                    if (str1.equals("ALARM")) break;
                    break;
                case 1644916852:
                    if (!str1.equals("HISTORY")) {
//                        break label601;

                        if (logger.isDebugEnabled()) {
                            logger.debug("收到一条报警消息:" + message);
                        }

                        Event alarmEvent = new SimpleEvent();
                        Map alarmHeader = new HashMap();
                        alarmHeader.put("CHANNEL_KEY", "ALARM");
                        if (compress.equalsIgnoreCase("true")) {
                            alarmHeader.put("COMPRESS", "true");
                        }
                        alarmEvent.setHeaders(alarmHeader);
                        alarmEvent.setBody(event.getBody());
                        eventList.add(alarmEvent);
                        break;
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("收到一条实时信息消息:" + message);
                        }

                        Event realInfoEvent = new SimpleEvent();
                        Map realInfoHeader = new HashMap();
                        realInfoHeader.put("CHANNEL_KEY", "REALINFO");
                        if (compress.equalsIgnoreCase("true")) {
                            realInfoHeader.put("COMPRESS", "true");
                        }
                        realInfoEvent.setHeaders(realInfoHeader);
                        realInfoEvent.setBody(event.getBody());
                        eventList.add(realInfoEvent);
//                        break ;

                        if (logger.isDebugEnabled()) {
                            logger.debug("收到一条转发记录消息:" + message);
                        }

                        Event forwardEvent = new SimpleEvent();
                        Map forwardEventHeader = new HashMap();
                        forwardEventHeader.put("CHANNEL_KEY", "FORWARD");
                        if (compress.equalsIgnoreCase("true")) {
                            forwardEventHeader.put("COMPRESS", "true");
                        }
                        forwardEvent.setHeaders(forwardEventHeader);
                        forwardEvent.setBody(event.getBody());
                        eventList.add(forwardEvent);
                    }
                    break;
            }
            label601:
            logger.error("收到一条未知类型消息:" + MESSAGETYPE + "," + message);

//            label635:
            Event hdfsEvent = createHdfsEvent(keyValues);
            if (hdfsEvent != null)
                eventList.add(hdfsEvent);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("ThriftSourceHandler append():" + e.getMessage(), e);
        }
        return eventList;
    }
}
