package com.adintellig.hbase.type;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

	private byte[] family = null;
	private byte[] qualifier = null;
	private byte[] cellValue = null;
	private byte[] rowkey = null;
	private long ts = System.currentTimeMillis();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			String lineString = value.toString();
			String[] arr = lineString.split("\t", -1);
			if (arr.length == 2) {
				String midTs = arr[0];
				String cfq = arr[1];
				String[] keys = midTs.split("\u0001", -1);
				if (keys.length == 2) {
					rowkey = Bytes.toBytes(keys[0]);
					ts = Long.parseLong(keys[1]);
				}

				String[] vals = cfq.split("\u0001", -1);
				if (vals.length == 3) {
					family = Bytes.toBytes(vals[0]);
					qualifier = Bytes.toBytes(vals[1]);
					cellValue = Bytes.toBytes(vals[2]);

				}

				Put put = new Put(rowkey, ts);
				put.add(family, qualifier, cellValue);

				context.write(new ImmutableBytesWritable(rowkey), put);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
