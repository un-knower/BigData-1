package com.hushiwei.mr.simpleJavaAPI;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author 小讲
 * @function 将指定格式的多个文件上传至 HDFS
 */
@SuppressWarnings("unused")
public class CopyManyFilesToHDFS {

  private static FileSystem fs = null;
  private static FileSystem local = null;

  /**
   * @function Main 方法
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    //文件上传路径
    Path dstPath = new Path("hdfs://dajiangtai:9000/middle/filter/");
    //调用文件上传 list 方法
    list(dstPath);
  }

  /**
   * function 过滤文件格式   将多个文件上传至 HDFS
   *
   * @param dstPath 目的路径
   */
  public static void list(Path dstPath) throws IOException, URISyntaxException {
    //读取hadoop文件系统的配置
    Configuration conf = new Configuration();
    //HDFS 接口
    URI uri = new URI("hdfs://dajiangtai:9000");
    //获取文件系统对象
    fs = FileSystem.get(uri, conf);
    // 获得本地文件系统
    local = FileSystem.getLocal(conf);
    //只上传data/testdata 目录下 txt 格式的文件
    FileStatus[] localStatus = local
        .globStatus(new Path("E:/DHDemo/data/*"), new RegexAcceptPathFilter("^.*txt$"));
    // 获得所有文件路径
    Path[] listedPaths = FileUtil.stat2Paths(localStatus);
    for (Path p : listedPaths) {
      //将本地文件上传到HDFS
      fs.copyFromLocalFile(p, dstPath);
    }
  }

  /**
   * @author 小讲
   * @function 只接受 txt 格式的文件aa
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
      //只接受 regex 格式的文件
      return flag;
    }
  }
}

