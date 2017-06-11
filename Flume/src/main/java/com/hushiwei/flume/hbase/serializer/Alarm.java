package com.hushiwei.flume.hbase.serializer;

import com.hushiwei.flume.utils.CompressUtil;
import com.hushiwei.flume.utils.Utils;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;



/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
public class Alarm implements HbaseEventSerializer {
    private static final Logger logger = Logger.getLogger(Alarm.class);
    private List<Row> list;

    public void configure(Context context)
    {
    }

    public void configure(ComponentConfiguration conf)
    {
    }

    public void initialize(Event event, byte[] columnFamily)
    {
        this.list = new ArrayList(1);
        try {
            String COMPRESS = StringUtils.trimToEmpty((String)event.getHeaders().get("COMPRESS"));
            String message = null;
            if (COMPRESS.equalsIgnoreCase("true"))
            {
                message = CompressUtil.uncompress(event.getBody());
            }
            else {
                message = Bytes.toString(event.getBody());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Alarm.initialize() 收到一条消息:" + message);
            }
            Map<String, String> keyValues = Utils.processMessageToMap(message);

            String ALARM_ID = StringUtils.trimToEmpty((String)keyValues.remove("ALARM_ID"));

            String[] alarmIdField = ALARM_ID.split("_", 3);
            ALARM_ID = alarmIdField[0] + "_" + Utils.convertToUTC(alarmIdField[1]) + "_" + alarmIdField[2];

            String TIME = StringUtils.trimToEmpty((String)keyValues.remove("TIME"));
            String STATUS = StringUtils.trimToEmpty((String)keyValues.get("STATUS"));
            if (STATUS.equals("1"))
            {
                keyValues.put("STARTTIME", TIME);
            } else if (STATUS.equals("3"))
            {
                keyValues.put("ENDTIME", TIME);
            } else {
                logger.error("未识别的报警STATUS：" + STATUS);
                return;
            }
            long timestamp = Utils.convertToUTC(TIME);
            Put put = new Put(Bytes.toBytes(ALARM_ID));
            for (Map.Entry entry : keyValues.entrySet()) {
                put.addColumn(columnFamily, Bytes.toBytes((String)entry.getKey()),
                        Bytes.toBytes((String)entry.getValue()));
            }
            this.list.add(put);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.error("Alarm.initialize() 出错:" + e.getMessage(), e);
        }
    }

    public List<Row> getActions() {
        return this.list;
    }

    public List<Increment> getIncrements()
    {
        return new ArrayList();
    }

    public void close()
    {
    }
}
