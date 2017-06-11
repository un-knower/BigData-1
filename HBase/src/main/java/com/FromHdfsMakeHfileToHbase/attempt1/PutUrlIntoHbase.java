package com.FromHdfsMakeHfileToHbase.attempt1;

/**
 * Created by hushiwei on 2017/6/2.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutUrlIntoHbase extends Configured implements Tool {
    static Logger logger = LoggerFactory.getLogger(PutUrlIntoHbase.class);


    private static final String MAPRED_JOB_NAME = "mapred.job.name";
    private static final String HBASE_TABLE = "mapred.job.name";

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum", quorum);
//        conf.set("hbase.coll.dt", day);
//        conf.set("hbase.tbname.target", targetTable);
        String inputPath = args[0];
        String outputPath = args[1];
        HTable hTable = null;
        int result = 0;
        try {
            Job job = Job.getInstance(conf, "ExampleRead");
            job.setJarByClass(PutUrlIntoHbase.class);
            job.setMapperClass(PutUrlIntoHbaseMapper.class);
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
            result = job.waitForCompletion(true) == true ? 0 : 1;
            if (result == 0) {
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
                return 0;
            } else {
                logger.error("loading failed.");
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                hTable.close();
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PutUrlIntoHbase(), args);
        System.exit(res);
    }
}
