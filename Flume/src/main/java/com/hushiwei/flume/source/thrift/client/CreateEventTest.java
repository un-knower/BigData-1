package com.hushiwei.flume.source.thrift.client;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class CreateEventTest
{
    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            Map headers = new HashMap();
            String message = "VID:111,VIN:LVBV4J0B2AJ063987,TIME:20150717173636,TYPE:LOGIN,1001:20150710173636,VTYPE:110,1002:京A12345,2002:550,2408:25,2001:1000";
            try {
                headers.put("VID", "111");
                headers.put("VIN", "LVBV4J0B2AJ063987");
                headers.put("TIME", "20150710173636");
                headers.put("DATATYPE", "REALINFO");
                Event localEvent = EventBuilder.withBody(message.getBytes("UTF-8"), headers);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
