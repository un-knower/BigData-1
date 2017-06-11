package com.hushiwei.mr.GdPro.mr;

import com.hushiwei.mr.GdPro.utils.HbaseUtils;
import com.hushiwei.mr.GdPro.utils.MyDateUtils;
import org.apache.commons.lang.StringUtils;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DataCountHbase {
    private static FileSystem getFileSystem()throws Exception{
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://ttyc102:9000");
        FileSystem fileSystem = FileSystem.get(uri,conf);
        return fileSystem;
    }

    //执行命令：hadoop jar /usr/local/apps/DataCount.jar /staging/JCStock/app.user.access/*/*.log /staging/JCStock/app.user.access/result24/ /staging/JCStock/app.user.access/result25/
    public static void main(String[] args)throws Exception{
        FileSystem fileSystem = getFileSystem();
        if(fileSystem.exists(new Path(args[1]))){
            fileSystem.delete(new Path(args[1]),true);
        }
    	/*FileSystem fileSystem = getFileSystem();
    	deleteFiles(fileSystem, args[0]);*/
    	
        
        Configuration conf = new Configuration();
        String jobName = DataCountHbase.class.getSimpleName();
		Job job = Job.getInstance(conf,jobName);
        job.setJarByClass(DataCountHbase.class);

        FileInputFormat.setInputPaths(job,args[0]);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(DataCountMapper.class);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(DataCountReducer.class);

        job.waitForCompletion(true);
        
        Job job2 = Job.getInstance(conf, jobName);
        job2.setJarByClass(DataCountHbase.class);
        
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(DataCountMapper2.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setReducerClass(DataCountReducer2.class);
        job2.waitForCompletion(true);
    }

    public static class DataCountMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        @Override
        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
        	String[] splited = value.toString().split(",");
        	String userid = null;
        	String requestTime = null;
        	if(splited!=null && splited.length==14){
        		userid = splited[4];
        		requestTime = splited[7];
        		if(userid!=null && !"".equals(userid) && 
        				requestTime!=null && !"".equals(requestTime)){
        			context.write(new Text(userid),new Text(getTimeSlot(requestTime)));
        		}
        	}
        }

		private String getTimeSlot(String requestTime) {
			String ymd = null;
			String date = null;
			if(requestTime!=null){
				String[] splited = requestTime.split(" ");
				ymd = splited[0];
				date = splited[1];
				if(getTimeSeconds("6:00")<=getTimeSeconds(date) && getTimeSeconds(date)<getTimeSeconds("9:30")){
					date = ymd+" 6:00-9:30";
				}else if(getTimeSeconds("9:30")<=getTimeSeconds(date) && getTimeSeconds(date)<getTimeSeconds("11:30")){
					date = ymd+" 9:30-11:30";
				}else if(getTimeSeconds("11:30")<=getTimeSeconds(date) && getTimeSeconds(date)<getTimeSeconds("13:00")){
					date = ymd+" 11:30-13:00";
				}else if(getTimeSeconds("13:00")<=getTimeSeconds(date) && getTimeSeconds(date)<getTimeSeconds("18:00")){
					date = ymd+" 13:00-18:00";
				}else if(getTimeSeconds("18:00")<=getTimeSeconds(date) && getTimeSeconds(date)<getTimeSeconds("24:00")){
					date = ymd+" 18:00-24:00";
				}
			}
			return date;
		}

		private int getTimeSeconds(String date) {
			int seconds = 0;
			String[] splited = date.split(":");
			if(splited!=null){
				if(splited.length==3){
					seconds = Integer.valueOf(splited[0])*3600+Integer.valueOf(splited[1])*60+Integer.valueOf(splited[2]);
				}else if(splited.length==2){
					seconds = Integer.valueOf(splited[0])*3600+Integer.valueOf(splited[1])*60;
				}
			}
			return seconds;
		}
    }

    public static class DataCountReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text k2,Iterable<Text> v2s,Context context)
            throws IOException,InterruptedException {
        	Map<String, Integer> map = new HashMap<String,Integer>();
        	Integer j = 0;
            for(Text v2: v2s){
            	Integer i = map.get(v2.toString());
            	if(i==null){
            		j = 1;
            		i = 0;
            	}
            	
            	if(map!=null && v2!=null){
            		map.put(v2.toString(), i+j);
            	}
            }
            
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                context.write(k2, new Text(entry.getKey()+"\t"+entry.getValue()));
                //String rowkey,String cf,String tableName,String c1,String d1,String c2,String d2,String c3,String d3
                new HbaseUtils().addOneRecord(
                		StringUtils.reverse(MyDateUtils.formatDate9(new Date())),
                		"info", "gdtb", "userid", k2.toString(), 
                		"dateslot", entry.getKey(), "count", String.valueOf(entry.getValue()));
            }
            map.clear();
        }
    }
    
    
    //(3944, 2016-09-06 18:00-24:00	3)
    public static class DataCountMapper2 extends Mapper<LongWritable,Text,Text,Text>
    {
        @Override
        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
        	String[] splited = value.toString().split("\t");
        	String userid = null;
        	String timeSlot = null;
        	String count = null;
        	String ymd = null;
        	String date = null;
        	if(splited!=null && splited.length==3){
        		userid = splited[0];
        		timeSlot = splited[1];
        		count = splited[2];
        		
        		if(timeSlot!=null && !"".equals(timeSlot)){
        			String[] splitedd = timeSlot.split(" ");
        			if(splitedd!=null && splitedd.length==2){
        				ymd = splitedd[0];
        				date = splitedd[1];
        			}
        		}
        		
        		context.write(new Text(userid+" "+ymd),new Text(timeSlot+"\t"+count));
        	}
        }
    }

    public static class DataCountReducer2 extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text k2,Iterable<Text> v2s,Context context)
            throws IOException,InterruptedException {
        	int max = 0;
        	String timeSlot = null;
            for(Text v2: v2s){
            	String[] splited = v2.toString().split("\t");
            	if(splited!=null && splited.length==2){
            		Integer i = Integer.valueOf(splited[1]);
					if(i>max){
            			max = i;
            			timeSlot = splited[0];
            		}
            	}
            }
            
            String str = k2.toString();
            String key2 = null;
			if(str!=null && !"".equals(str)){
				String[] splitedd = str.split(" ");
				key2 = splitedd[0];
			}
            
            context.write(new Text(key2), new Text(timeSlot+"\t"+max));
        }
    }
}