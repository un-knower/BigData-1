package com.hsw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static com.hsw.Constants.*;


/**
 * Created by hushiwei on 2017/3/31.
 */
public class HbaseGetUtil {
    public static void main(String[] args) throws IOException {
        Configuration conf= HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.10.25.204,10.10.25.205,10.10.25.206");
        try {
            HTable dmp_wanka = new HTable(conf, "dmp_wanka");
            HConnection connection = HConnectionManager.createConnection(conf);
//            HTableInterface dmp_wanka = connection.getTable("dmp_wanka");
            Get get = new Get(Bytes.toBytes("ec9b29d718f79a7905ebcd384c182d1c"));
            System.out.println(dmp_wanka.isAutoFlush());
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_imei));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_channel_id));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_channel));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_country));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_province));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_city));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_manu));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_model));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_net));
            get.addColumn(Bytes.toBytes(cf_info), Bytes.toBytes(col_ovr));
            Result result = dmp_wanka.get(get);
            byte[] rowKey = result.getRow();
            byte[] imeiBytes = null;
            if (rowKey != null && rowKey.length > 0) {
                imeiBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_imei));
                byte[] chidBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_channel_id));
                byte[] channelBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_channel));
                byte[] coBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_country));
                //byte[] pbytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_province));
                byte[] cbytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_city));
                byte[] manuBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_manu));
                byte[] modelBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_model));
                byte[] netBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_net));
                byte[] ovrBytes = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_ovr));
                String imeimd5 = new String(imeiBytes);
                String channelid = new String(chidBytes);
                String channel = new String(channelBytes);

                System.out.println(imeimd5+" -- "+channelid+" -- "+channel);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
