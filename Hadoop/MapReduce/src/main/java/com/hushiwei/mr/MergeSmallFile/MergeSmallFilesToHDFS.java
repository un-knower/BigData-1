package com.hushiwei.mr.MergeSmallFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
/**
 * function 合并小文件至 HDFS 
 * @author 小讲
 *
 */
public class MergeSmallFilesToHDFS {
	private static FileSystem fs = null;
	private static FileSystem local = null;
	/**
	 * @function main 
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		list();
	}

	/**
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void list() throws IOException, URISyntaxException {
		// 读取hadoop文件系统的配置
		Configuration conf = new Configuration();
		//文件系统访问接口
		URI uri = new URI("hdfs://ncp161:8020");
		//创建FileSystem对象aa
		fs = FileSystem.get(uri, conf);
		// 获得本地文件系统
		local = FileSystem.getLocal(conf);
		//过滤目录下的 svn 文件
		FileStatus[] dirstatus = local.globStatus(new Path("E:\\bigdataData\\sample-tvdata\\*"),new RegexExcludePathFilter("^.*svn$"));
		//获取73目录下的所有文件路径
		Path[] dirs = FileUtil.stat2Paths(dirstatus);
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		for (Path dir : dirs) {
			String fileName = dir.getName().replace("-", "");//文件名称
			//只接受日期目录下的.txt文件a
			FileStatus[] localStatus = local.globStatus(new Path(dir+"/*"),new RegexAcceptPathFilter("^.*txt$"));
			// 获得日期目录下的所有文件
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			//输出路径
			Path block = new Path("hdfs://ncp161:8020/hsw/middle/sampleTv/"+ fileName + ".txt");
			// 打开输出流
			out = fs.create(block);			
			for (Path p : listedPaths) {
				in = local.open(p);// 打开输入流
				IOUtils.copyBytes(in, out, 4096, false); // 复制数据
				// 关闭输入流
				in.close();
			}
			if (out != null) {
				// 关闭输出流a
				out.close();
			}
		}
		
	}

	/**
	 * 
	 * @function 过滤 regex 格式的文件
	 *
	 */
	public static class RegexExcludePathFilter implements PathFilter {
		private final String regex;

		public RegexExcludePathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return !flag;
		}

	}

	/**
	 * 
	 * @function 接受 regex 格式的文件
	 *
	 */
	public static class RegexAcceptPathFilter implements PathFilter {
		private final String regex;

		public RegexAcceptPathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return flag;
		}

	}
}
