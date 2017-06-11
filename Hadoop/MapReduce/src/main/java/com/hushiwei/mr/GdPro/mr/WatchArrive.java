package com.hushiwei.mr.GdPro.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class WatchArrive {
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
        String jobName = WatchArrive.class.getSimpleName();
		Job job = Job.getInstance(conf,jobName);
        job.setJarByClass(WatchArrive.class);

        FileInputFormat.setInputPaths(job,args[0]);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(WatchArriveMapper.class);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(WatchArriveReducer.class);

        job.waitForCompletion(true);
    }

    public static class WatchArriveMapper extends Mapper<LongWritable,Text,Text,Text>
    {
    	
    	//stbNum+"@"+date+"@"+e+"@"+s+"@"+p+"@"+sn+"@"+(ess-sss)

		@Override
        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
        	String[] splited = value.toString().split("@");
        	String stbNum = null;
        	String date = null;
        	String e = null;
        	String s = null;
        	String p = null;
        	String sn = null;
        	if(splited!=null&&splited.length>0){
        		stbNum = splited[0];
        		date = splited[1];
        		e = splited[2];
        		s = splited[3];
        		p = splited[4];
        		sn = splited[5];
        	}
        	List<String> list = TimeUtil.getTimeSplit(e,s);
        }
    }

	    public static class WatchArriveReducer extends Reducer<Text,Text,Text,Text>{
	        @Override
	        protected void reduce(Text k2,Iterable<Text> v2s,Context context)
	            throws IOException,InterruptedException {
	            for(Text v2: v2s){
	            	context.write(new Text(""),v2);
	            }
	        }
	    }
}