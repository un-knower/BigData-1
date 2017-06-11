package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Random;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import sun.misc.BASE64Encoder;

public class Producer
{
    static int a = 0;
    private static Logger logger = Logger.getLogger(Producer.class);
    private KafkaProducer producer1;
    private KafkaProducer producer2;

    public void before()
    {
        Properties props = new Properties();

        props.put("metadata.broker.list", "10.8.6.164:9092");
        props.put("request.required.acks", "1");
        props.put("producer.type", "sync");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");

        ProducerConfig config = KafkaProducerConfig.createProducerConfig(props);
        this.producer1 = new KafkaProducer("test_forward", config);
        this.producer2 = new KafkaProducer("lhy_topic2", config);
    }

    public void testSendLogin() throws Exception {
        Random rd = new Random();
        String[] Login = { "SUBMIT  1  LVBV4J0B2AJ063987  LOGIN  {1001:20150623120000,1002:京A12345}",
                "SUBMIT  1  LVBV4J0B2AJ063988  LOGIN  {1001:20150623120000,1002:京A12346}",
                "SUBMIT  1  LVBV4J0B2AJ063989  LOGIN  {1001:20150623120000,1002:京A12347}",
                "SUBMIT  1  LVBV4J0B2AJ063990  LOGIN  {1001:20150623120000,1002:京A12348}",
                "SUBMIT  1  LVBV4J0B2AJ063991  LOGIN  {1001:20150623120000,1002:京A12349}" };
        String[] key = { "LVBV4J0B2AJ063987", "LVBV4J0B2AJ063988", "LVBV4J0B2AJ063989", "LVBV4J0B2AJ063990", "LVBV4J0B2AJ063991" };
        for (int i = 0; i < 10; i++) {
            int index = rd.nextInt(Login.length);
            byte[] data = Login[index].getBytes("UTF-8");
            this.producer1.send(data);
            this.producer2.sendKeyMessage(key[index].getBytes("UTF-8"), data);
            logger.info("发送一条kafka消息:" + Login);
        }

        this.producer1.close();
    }

    public void testSendForward()
            throws Exception
    {
        String forward = "SUBMIT  1  LVBV4J0B2AJ063987  FORWARD  {VID:111,VTYPE:3,TIME:20150720151510,FLAG:1,TYPE:1,RESULT:1}";

        this.producer1.send(forward.getBytes());
    }

    public void testSendPacket()
            throws Exception
    {
        for (int i = 0; i < 30; i++) {
            new Thread()
            {
                public void run()
                {
                    BASE64Encoder encoder = new BASE64Encoder();
                    String base64 = null;
                    try {
                        base64 = encoder.encode("京A12345".getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                    Random rd = new Random();
                    String[] Login = { "SUBMIT  1  LVBV4J0B2AJ063987  PACKET  {1:0x01,2:" +
                            base64 + ",3:20150623120000" };
                    for (int i = 0; i < 100000; i++) {
                        byte[] data = null;
                        try {
                            data = (Login[0] + ",index:" + i + "}")
                                    .getBytes("UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        Producer.this.producer1.send(data);
                        Producer.this.producer2.send("SUBMIT  1  LVBV4J0B2AJ063987  LOGIN  {1:0x01,2:5LqsQTEyMzQ1,1001:20150623120000,index:1669}".getBytes());

                        Producer.a += 1;
                    }
                }
            }

                    .start();
        }
        int hist = 0;
        while (true) {
            System.out.println(a);
            System.out.println(a - hist);
            hist = a;
            Thread.sleep(1000L);
        }
    }

    public static void main(String[] args) {
        Producer p = new Producer();
        p.before();
        try {
            for (int i = 0; i < 1; i++)
                p.testSendForward();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
