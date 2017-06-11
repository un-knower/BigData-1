package com.FromHdfsMakeHfileToHbase.attempt3;

/**
 * Created by hushiwei on 2017/6/2.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * 测试用法:
 * 1.建hbase测试表
 * create 'hfiletable','fm1','fm2'
 *
 * 2.准备输入数据
 * key1,fm1:col1,value1
 key1,fm1:col2,value2
 key1,fm2:col1,value3
 key4,fm1:col1,value4
 *
 *3.执行程序
 *
 *hadoop jar dmp_aggregated_data-1.0-SNAPSHOT-jar-with-dependencies.jar com.FromHdfsMakeHfileToHbase.attempt3.BulkLoadJob /user/hsw/hbasedata1.txt /user/hsw/output hfiletable
 *
 */
public class BulkLoadJob {
    static Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);

    public static class BulkLoadMap extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] valueStrSplit = value.toString().split(",");
            String hkey = valueStrSplit[0];
            String family = valueStrSplit[1].split(":")[0];
            String column = valueStrSplit[1].split(":")[1];
            String hvalue = valueStrSplit[2];
            final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
            Put HPut = new Put(rowKey);
            HPut.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(hvalue));
            context.write(HKey, HPut);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum", quorum);
//        conf.set("hbase.coll.dt", day);
//        conf.set("hbase.tbname.target", targetTable);
        String inputPath = args[0];
        String outputPath = args[1];
        HTable hTable = null;
        try {
            Job job = Job.getInstance(conf, "ExampleRead");
            job.setJarByClass(BulkLoadJob.class);
            job.setMapperClass(BulkLoadJob.BulkLoadMap.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setReducerClass(PutSortReducer.class);

            // speculation
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
            // in/out format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            hTable = new HTable(conf, args[2]);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable);

            if (job.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", args[1]});
                } catch (Exception e) {
                    logger.error("Couldnt change the file permissions ", e);
                    throw new IOException(e);
                }
                //加载到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(new Path(outputPath), hTable);
            } else {
                logger.error("loading failed.");
                System.exit(1);
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                hTable.close();
            }
        }
    }
}
