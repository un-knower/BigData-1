package com.hushiwei.flume.hbase.serializer;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hushiwei.flume.utils.CompressUtil;
import com.hushiwei.flume.utils.Utils;
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

public class RealInfo
        implements HbaseEventSerializer
{
    private static final Logger logger = Logger.getLogger(RealInfo.class);
    private List<Row> list;
    private boolean compress;

    public void configure(Context context)
    {
        this.compress = context.getBoolean("compress", Boolean.valueOf(true)).booleanValue();
    }

    public void configure(ComponentConfiguration conf)
    {
    }

    public void initialize(Event event, byte[] columnFamily)
    {
        try
        {
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
                logger.debug("RealInfo.class initialize()收到一条消息:" + message);
            }

            Map<String, String> keyValues = Utils.processMessageToMap(message);
            String VID = (String)keyValues.remove("VID");

            String TIME = (String)keyValues.remove("TIME");

            logger.info("realinfo收到一条数据:" + VID + "--" + TIME);

            this.list = new ArrayList(1);
            long timestamp = Utils.convertToUTC(TIME);
            Put put = new Put(Bytes.toBytes(VID + "_" + timestamp));

            for (Map.Entry entry : keyValues.entrySet()) {
                if ((this.compress) && ((((String)entry.getKey()).equals("2003")) || (((String)entry.getKey()).equals("2103"))))
                {
                    put.addColumn(columnFamily, Bytes.toBytes((String)entry.getKey()),
                            CompressUtil.compress((String)entry.getValue()));
                }
                else put.addColumn(columnFamily, Bytes.toBytes((String)entry.getKey()),
                        Bytes.toBytes((String)entry.getValue()));
            }

            this.list.add(put);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("RealInfo.class initialize()出错:" + e.getMessage(), e);
        }
    }

    public List<Row> getActions()
    {
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
