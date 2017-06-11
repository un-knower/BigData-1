package com.hushiwei.flume.hbase.serializer;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import sun.misc.BASE64Decoder;

public class ErrorPacket
        implements HbaseEventSerializer
{
    private List<Row> list;
    private static Logger logger = Logger.getLogger(ErrorPacket.class);
    private BASE64Decoder decoder = new BASE64Decoder();

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
            String message = Bytes.toString(event.getBody());
            if (logger.isDebugEnabled()) {
                logger.info(getClass().getName() + "收到一条错误的原始报文消息:" + message);
            }

            String[] fields = message.split("\\s+");

            String json = fields[4];

            String[] jsonFields = json.substring(1, json.length() - 1)
                    .split(",");

            Map jsonMap = new HashMap();
            for (String keyValue : jsonFields) {
                String[] kv = keyValue.split(":");
                jsonMap.put(kv[0], kv[1]);
            }
            String TYPE = StringUtils.trimToEmpty((String)jsonMap.get("1"));
            long timestamp = Utils.convertToUTC(StringUtils.trimToEmpty((String)jsonMap.get("3")));
            String VIN = StringUtils.trimToEmpty(fields[2]);
            String VERIFY = StringUtils.trimToEmpty((String)jsonMap.get("4"));

            Put put = new Put((VIN + "_" + timestamp + "_" + TYPE).getBytes());
            put.addColumn(columnFamily, Bytes.toBytes("data"), this.decoder.decodeBuffer((String)jsonMap.get("2")));
            put.addColumn(columnFamily, Bytes.toBytes("type"), Bytes.toBytes(TYPE));
            put.addColumn(columnFamily, Bytes.toBytes("verify"), Bytes.toBytes(VERIFY));
            this.list.add(put);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Packet", e);
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
