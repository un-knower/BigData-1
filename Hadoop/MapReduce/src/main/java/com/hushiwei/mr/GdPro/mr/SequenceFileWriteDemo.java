package com.hushiwei.mr.GdPro.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFileWriteDemo {
	private static final String[] DATA = {
	    "One, two, buckle my shoe",
	    "Three, four, shut the door",
	    "Five, six, pick up sticks",
	    "Seven, eight, lay them straight",
	    "Nine, ten, a big fat hen"
	};

	public static void main(String[] args) throws IOException{
	    String url = "D:/kkk/dt=2016-09-05/in1";
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(url),conf);
	    Path path = new Path(url);

	    IntWritable key = new IntWritable();
	    Text value = new Text();
	    SequenceFile.Writer writer = null;
	    try{
	        writer = SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());
	        for (int i=0;i<1024;i++){
	            key.set(1024-i);
	            value.set(DATA[i % DATA.length]);
	            writer.append(key,value);
	        }
	    } finally{
	        IOUtils.closeStream(writer);
	    }
	}
}
