package com.hushiwei.mr.GdPro.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SmallFilesToSequenceFileConverter {
	 static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {  
	        private Text filenameKey;  
	          
	        @Override  
	        protected void setup(Context context) throws IOException,  
	        InterruptedException {  
	            InputSplit split = context.getInputSplit();  
	            Path path = ((FileSplit)split).getPath();  
	            filenameKey = new Text(path.toString());  
	        }  
	  
	        @Override  
	        protected void map(NullWritable key, BytesWritable value,  
	                Context context) throws IOException, InterruptedException {  
	            context.write(filenameKey, value);  
	        }  
	  
	    }  
	      
	    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {  
	        Configuration conf = new Configuration();
	        Job job = new Job(conf,"fdaf");
	          
	        FileInputFormat.setInputPaths(job, new Path("D:/kkk/dt=2016-09-05"));
	        FileOutputFormat.setOutputPath(job, new Path("D:/kkk/dt=2016-09-05/out"));
	        job.setJarByClass(SmallFilesToSequenceFileConverter.class);
	        job.setInputFormatClass(WholeFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(BytesWritable.class);
	        job.setMapperClass(SequenceFileMapper.class);
	        job.waitForCompletion(true);
	    }
}
