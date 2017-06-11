package com.hushiwei.mr.NCDCNOAA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by HuShiwei on 2016/9/2 0002.
 * 统计美国各个气象站30年来的平均气温
 */
/*
1985 07 31 02   200    94 10137   220    26     1     0 -9999
1985 07 31 03   172    94 10142   240     0     0     0 -9999
1985 07 31 04   156    83 10148   260    10     0     0 -9999
1985 07 31 05   133    78 -9999   250     0 -9999     0 -9999
1985 07 31 06   122    72 -9999    90     0 -9999     0     0
1985 07 31 07   117    67 -9999    60     0 -9999     0 -9999
1985 07 31 08   111    61 -9999    90     0 -9999     0 -9999
1985 07 31 09   111    61 -9999    60     5 -9999     0 -9999
1985 07 31 10   106    67 -9999    80     0 -9999     0 -9999
1985 07 31 11   100    56 -9999    50     5 -9999     0 -9999
 */
public class Temperature extends Configured implements Tool {
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * @function Mapper 解析气象站数据
         * @input key=偏移量  value=气象站数据
         * @output key=weatherStationId value=temperature
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            每行气象数据
            final String line = value.toString();
//            每小时气温值
            final int temperature = Integer.parseInt(line.substring(14, 19).trim());
//            过滤无效数据
            if (temperature != -9999) {
                final FileSplit fileSplit = (FileSplit) context.getInputSplit();
//                通过文件名称提取气象站id
                final String weatherStationId = fileSplit.getPath().getName().substring(5, 10);
                context.write(new Text(weatherStationId), new IntWritable(temperature));

            }
        }
    }

    /*
    在 map 的输出结果中，所有相同的气象站（key）被分配到同一个reduce执行，
    这个平均气温就是针对同一个气象站（key），通过循环所有气温值（values）求和并求平均数所得到的。
     */
    public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        final IntWritable result = new IntWritable();

        /**
         * @function Reducer 统计美国各个气象站的平均气温
         * @input key=weatherStationId  value=temperature
         * @output key=weatherStationId value=average(temperature)
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
//            统计每个气象站的气温值总和
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
//            求每个气象站的气温平均值
            result.set(sum / count);
            context.write(key, result);
        }
    }


    //    负责运行 MapReduce 作业。
    @Override
    public int run(String[] args) throws Exception {
//        1.读取配置文件
        final Configuration conf = new Configuration();
        final Path mypath = new Path(args[1]);
        final FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.exists(mypath)) {
            hdfs.delete(mypath, true);

        }
//        2.新建一个任务
        final Job job = new Job(conf, "temperature");
//        3.设置主类
        job.setJarByClass(Temperature.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));//输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        job.setMapperClass(TemperatureMapper.class);//Mapper
        job.setReducerClass(TemperatureReducer.class);//Reducer

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //    hadoop jar ./MapReduce-1.0-SNAPSHOT-jar-with-dependencies.jar  com.hushiwei.mr.NCDCNOAA.Temperature
    public static void main(String[] args) throws Exception {
        String[] args0 = {
                "hdfs://ncp161:8020/hsw/weather/",
                "hdfs://ncp161:8020/hsw/weather/out/"
        };
        int exitCode = ToolRunner.run(new Configuration(), new Temperature(), args0);
        System.exit(exitCode);
    }
}
