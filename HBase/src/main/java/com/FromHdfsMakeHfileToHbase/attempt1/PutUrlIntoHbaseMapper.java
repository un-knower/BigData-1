package com.FromHdfsMakeHfileToHbase.attempt1;

/**
 * Created by hushiwei on 2017/6/2.
 */

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutUrlIntoHbaseMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    static Logger logger = LoggerFactory.getLogger(PutUrlIntoHbaseMapper.class);


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            context.getCounter("user_mapper", "NULL").increment(1L);
            return;
        }

        try {
            String[] valueStrSplit = value.toString().split(",", -1);
            if (valueStrSplit.length != 3) {
                context.getCounter("user_mapper", "INVALID").increment(1L);
                return;
            }

            byte[] hkey = Bytes.toBytes(valueStrSplit[0].trim()); // top level sitemap id
            byte[] family = Bytes.toBytes(valueStrSplit[1].split(":")[0]);
            byte[] column = Bytes.toBytes(valueStrSplit[1].split(":")[1]);
            byte[] hvalue = Bytes.toBytes(valueStrSplit[2]);

            Put put = new Put(hkey); // key : url_sitemapType
            put.addImmutable(family, column, hvalue);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("smap_id"), sitemap_id);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("smap_type"), sitemap_type);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("top_smap"), top_level_sitemap);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("second_smap"), second_level_sitemap);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("first_parse_time"), parse_time);
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("select_ts"), Bytes.toBytes("0")); // default does not select
//            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"), Bytes.toBytes("0")); // default does not fetch
            context.getCounter("user_mapper", "INSERT ROW").increment(1L);
            context.write(new ImmutableBytesWritable(hkey), put);
        } catch (Exception e) {
            context.getCounter("user_mapper", "EXCEPTION").increment(1L);
        }

    }

}
