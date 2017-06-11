package com.hushiwei.mr.GdPro.mr;

import com.hushiwei.mr.GdPro.utils.JsoupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class DataClean {
    private static FileSystem getFileSystem()throws Exception{
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://ttyc102:9000");
        FileSystem fileSystem = FileSystem.get(uri,conf);
        return fileSystem;
    }

    //执行命令：bin/hadoop jar /root/softs/apps/DownloadAppTopN.jar /input/appdatas2 /2016041830 /2016041836
    public static void main(String[] args)throws Exception{
//        FileSystem fileSystem = getFileSystem();
//        if(fileSystem.exists(new Path(args[1]))){
//            fileSystem.delete(new Path(args[1]),true);
//        }
        
        Configuration conf = new Configuration();
        String jobName = DataClean.class.getSimpleName();
		Job job = Job.getInstance(conf,jobName);
        job.setJarByClass(DataClean.class);

        FileInputFormat.setInputPaths(job,args[0]);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(DataCleanMapper.class);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(DataCleanReducer.class);

        job.waitForCompletion(true);
    }

    public static class DataCleanMapper extends Mapper<LongWritable,Text,NullWritable,Text>
    {
        @Override
        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
        	
        	context.write(NullWritable.get(),new Text(JsoupUtils.getJsoup(value.toString())));
        }
    }

	    public static class DataCleanReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
	        @Override
	        protected void reduce(NullWritable k2,Iterable<Text> v2s,Context context)
	            throws IOException,InterruptedException {
	            for(Text v2: v2s){
	            	context.write(NullWritable.get(),v2);
	            }
	        }
	    }
}