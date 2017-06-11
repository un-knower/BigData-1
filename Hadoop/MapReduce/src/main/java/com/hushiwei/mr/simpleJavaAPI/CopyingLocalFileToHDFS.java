package com.hushiwei.mr.simpleJavaAPI;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author 小讲
 * @function Copying from Local file system to HDFS
 * 
 */
public class CopyingLocalFileToHDFS {
	/**
	 * @function Main() 方法
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		// 本地文件路径
		String source = "E:\\GitHub\\BigData\\Hadoop\\MapReduce\\src\\main\\resources\\weibo.txt";
		// hdfs文件路径
		String dest = "hdfs://ncp161:8020/hsw/weibo/";
		copyFromLocal(source, dest);
	}

	/**
	 * @functaion 本地文件上传至 HDFS
aa	 * @param source 原文件路径
	aa * @param dest  目的文件路径
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void copyFromLocal(String source, String dest)
			throws IOException, URISyntaxException {
		// 读取hadoop文件系统的配置
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://ncp161:8020");
		// FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
		FileSystem fileSystem = FileSystem.get(uri, conf);
		// 源文件路径
		Path srcPath = new Path(source);
		// 目的路径
		Path dstPath = new Path(dest);
		// 查看目的路径是否存在
		if (!(fileSystem.exists(dstPath))) {
			// 如果路径不存在，即刻创建
			fileSystem.mkdirs(dstPath);
		}
		// 得到本地文件名称
		String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
		try {
			// 将本地文件上传到HDFS
			fileSystem.copyFromLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + " copied to " + dest);
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}

}
