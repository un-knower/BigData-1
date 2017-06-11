package com.hushiwei.flume.source.thrift.client;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class FlumeThriftClient
{
    public static void main(String[] args)
            throws Exception
    {
        for (int i = 0; i < 100; i++) {
            new Client().start();
        }
        TimeUnit.MINUTES.sleep(10L);
    }

    private static class Client extends Thread
    {
        public void run()
        {
            RpcClient client = null;
            try {
                Properties props = new Properties();

                props.put("client.type", "thrift");
                props.put("protocol", "compact");
                props.put("hosts", "h1");
                props.put("hosts.h1", "192.168.16.165:8090");
                props.put("batch-size", Integer.valueOf(100));

                client = RpcClientFactory.getInstance(props);

                long start = System.currentTimeMillis();
                for (int i = 0; i < 999999999; i++) {
                    try {
                        Map headers = new HashMap();

                        String message = "VID:16884,VIN:000000000000016863,TIME:20150803153334,TYPE:REALTIME,2210:0,2608:2,VTYPE:24,2609:73,2606:4040,2607:1,2002:1,2604:1,2003:MTo0MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDQwXzQwNjBfNDA0MF80MDQwXzQwNDBfNDA2MF80MDQwXzQwNDBfNDA0MF80MDYwXzQwNDBfNDA2MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDQwXzQwNDBfNDA0MF80MDYwXzQwNDBfNDA0MF80MDQwXzQwNjBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDQwXzQwNDBfNDA2MF80MDYwXzQwNjBfNDA0MF80MDYwXzQwNjBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDQwXzQwNDBfNDA0MF80MDYwXzQwNDBfNDA0MF80MDQwXzQwNjBfNDA2MF80MDQwXzQwNDBfNDA2MF80MDQwXzQwNjBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDYwXzQwNjBfNDA2MF80MDYwXzQwNDBfNDA2MF80MDQw,2605:1,2602:12,2603:4060,2601:1,2505:0,2209:100,2208:0,2103:MTo3MV83M183Ml83MV83Ml83MV83MV83MV83MV83MV83Ml83MV83MV83MF83MF83MF82OV83MF83MF83MA==,2101:20,2102:1,2504:0,2203:16,2503:39902797,2202:54280,2502:116290406,2201:0,2501:0,2801:0,2615:212,2616:258,2617:10000,2611:17,2612:69,2613:3690,2808:0,2614:9920,2802:0,2804:0,2610:1,2303:0,2304:83,2305:3680,2306:10002,2301:1,2302:16,2001:91,2000:20150803153334";

                        headers.put("VID", "111");
                        headers.put("VIN", "LVBV4J0B2AJ063987");
                        headers.put("TIME", "20150710173636");
                        headers.put("DATATYPE", "REALINFO");
                        String message2 = message.replaceFirst("VID:111", "VID:" + (i + 1));
                        Event event = EventBuilder.withBody(message2, Charset.forName("UTF-8"), headers);
                        client.append(event);
                        System.out.println(i);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println(end - start);
            } finally {
                if (client != null)
                    client.close();
            }
        }
    }
}
