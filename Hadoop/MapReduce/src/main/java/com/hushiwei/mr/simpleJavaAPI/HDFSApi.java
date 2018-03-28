package com.hushiwei.mr.simpleJavaAPI;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by HuShiwei on 2016/9/2 0002.
 */
public class HDFSApi {

    /**
     * //获取文件系统
     * 如果放到 hadoop 集群上面运行，获取文件系统可以直接使用 FileSystem.get(conf)。
     */
    public static FileSystem getFileSystem() throws URISyntaxException, IOException {
        //读取配置文件
        Configuration conf = new Configuration();

        //返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
        //FileSystem fs = FileSystem.get(conf);

        //指定的文件系统地址
//        URI uri = new URI("hdfs://jusfoun2016:8020");
        URI uri = new URI("hdfs://master:8020");
        //返回指定的文件系统    如果在本地测试，需要使用此种方法获取文件系统
        FileSystem fs = FileSystem.get(uri, conf);

        return fs;

    }

    public static FileSystem getFileSystem(String url) throws URISyntaxException, IOException {
        //读取配置文件
        Configuration conf = new Configuration();

        //返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
        //FileSystem fs = FileSystem.get(conf);

        //指定的文件系统地址
        String urlStr = "hdfs://jusfoun2016:8020" + url;
        //返回指定的文件系统    如果在本地测试，需要使用此种方法获取文件系统
        FileSystem fs = FileSystem.get(URI.create(urlStr), conf);

        return fs;

    }

    /**
     * @param url
     * @param dstPath
     * @param fileName
     * @throws IOException        抛出异常
     * @throws URISyntaxException
     */
    public static void hdfs2local(String url, String dstPath, String fileName)
            throws IOException, URISyntaxException {
        //获取文件系统
//        /hsw/tvdata/2012-09-20/ars10768@20120921013000.txt
        FileSystem              fs     = getFileSystem(url);
        InputStream             in     = null;
        String                  urlStr = "hdfs://jusfoun2016:8020" + url;
        final FSDataInputStream input  = fs.open(new Path(urlStr));
        File                    file   = new File(dstPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        File newFile = new File(file, fileName);
        newFile.setReadable(true);
        newFile.setWritable(true);
        newFile.setExecutable(true);
        System.out.println("新文件的绝对路径:" + newFile.getAbsolutePath());
        ;
        System.out.println("新文件的路径:" + newFile.getPath());
        ;
        FileOutputStream output = new FileOutputStream(newFile);
        IOUtils.copyBytes(input, output, input.available(), true);
        output.close();
        input.close();

    }

    //创建文件目录
    public static void mkdir(String path) throws Exception {

        //获取文件系统
        FileSystem fs = getFileSystem();

        //创建文件目录
//        fs.mkdirs(new Path("hdfs://jusfoun2016:8020/middle/weibo"));
        fs.mkdirs(new Path(path));

        //释放资源
        fs.close();
    }

    //删除文件或者文件目录
    public static void rmdir(String path) throws Exception {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //删除文件或者文件目录  delete(Path f) 此方法已经弃用
//        fs.delete(new Path("hdfs://jusfoun2016:8020/middle/weibo"),true);
        fs.delete(new Path(path), true);

        //释放资源
        fs.close();
    }

    //获取目录下的所有文件
    public static void ListAllFile(String path) throws IOException, URISyntaxException {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //列出目录内容
//        FileStatus[] status = fs.listStatus(new Path("hdfs://cloud004:9000/middle/weibo/"));
        FileStatus[] status = fs.listStatus(new Path(path));

        //获取目录下的所有文件路径
        Path[] listedPaths = FileUtil.stat2Paths(status);

        //循环读取每个文件
        for (Path p : listedPaths) {

            System.out.println(p);

        }
        //释放资源
        fs.close();
    }

    //文件上传至 HDFS
    public static void copyToHDFS() throws IOException, URISyntaxException {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
        Path srcPath = new Path("/home/hadoop/djt/weibo.txt");

        // 目的路径
        Path dstPath = new Path("hdfs://cloud004:9000/middle/weibo");

        //实现文件上传
        fs.copyFromLocalFile(srcPath, dstPath);

        //释放资源
        fs.close();
    }

    //从 HDFS 下载文件
    public static void getFile() throws IOException, URISyntaxException {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //源文件路径
        Path srcPath = new Path("hdfs://jusfoun2016:8020/hsw/weather/30yr_03103.dat");

        //目的路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/
        Path dstPath = new Path("E:\\bigdataData\\weather\\ddd");

        //下载hdfs上的文件
        fs.copyToLocalFile(srcPath, dstPath);
        System.out.println("over");

        //释放资源
        fs.close();
    }

    //获取 HDFS 集群节点信息
    public static void getHDFSNodes() throws IOException, URISyntaxException {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //获取分布式文件系统
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;

        //获取所有节点
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        //循环打印所有节点
        for (int i = 0; i < dataNodeStats.length; i++) {
            System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
        }
    }

    //查找某个文件在 HDFS 集群的位置
    public static void getFileLocal() throws IOException, URISyntaxException {

        //返回FileSystem对象
        FileSystem fs = getFileSystem();

        //文件路径
        Path path = new Path("hdfs://jusfoun2016:8020/hsw/weather/30yr_03103.dat");

        //获取文件目录
        FileStatus filestatus = fs.getFileStatus(path);
        //获取文件块位置列表
        BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        //循环输出块信息
        for (int i = 0; i < blkLocations.length; i++) {
            String[] hosts = blkLocations[i].getHosts();
            System.out.println("block_" + i + "_location:" + hosts[0]);
        }
    }

    public static void readFile(String path) throws IOException, URISyntaxException {
        FileSystem  fs = getFileSystem();
        InputStream in = null;
        in = fs.open(new Path(path));

        ByteArrayOutputStream swapStram = new ByteArrayOutputStream();
        byte[]                buff      = new byte[100];//buffer用于存放循环读取的临时数据
        int                   rc        = 0;
        while ((rc = in.read(buff, 0, 100)) > 0) {
            swapStram.write(buff, 0, rc);
        }
        byte[] bytes = swapStram.toByteArray();
        byteAvro2str(bytes);
//
        System.out.println(new String(bytes));
//        File file = new File(new String(bytes));
//        byteAvro2str2(file);

//        IOUtils.copyBytes(in,System.out,4096,false);
        IOUtils.closeStream(in);

    }

    public static void byteAvro2str(byte[] str) {

        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("Kafka/avsc/test_schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
//        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
//        Decoder decoder = DecoderFactory.get().binaryDecoder(str, null);
//        GenericRecord payload2 = null;
//        try {
//            payload2 = reader.read(null, decoder);
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("Message received : " + payload2.get("data"));

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        GenericRecord                    record          = recordInjection.invert(str).get();

//        System.out.println("data= " + record.get("data")+"\n"+
//        "id"+record.get("id")+"\n"+
//        "desc"+record.get("desc"));
        System.out.println(record);

    }

    public static void byteAvro2str2(File file) throws IOException {
        String schemaStr = "{  \n" +
                "    \"namespace\": \"xyz.test\",  \n" +
                "     \"type\": \"record\",  \n" +
                "     \"name\": \"payload\",  \n" +
                "     \"fields\":[  \n" +
                "         {  \n" +
                "            \"name\": \"data\", \"type\": [\"null\",{\"type\":\"map\", \"values\":\"string\"}]\n"
                +
                "         },\n" +
                "         {  \n" +
                "            \"name\": \"id\",  \"type\": [\"int\", \"null\"]  \n" +
                "         },  \n" +
                "         {  \n" +
                "            \"name\": \"desc\", \"type\": [\"string\", \"null\"]  \n" +
                "         }  \n" +
                "     ]  \n" +
                "}  \n";

        Schema schema = null;
        schema = new Schema.Parser().parse(schemaStr);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,
                datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            try {
                user = dataFileReader.next(user);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(user);
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException {

        //mkdir();
        //rmdir();
        String path = "hdfs://master:8020/user/hsw/aa.txt";

        readFile(path);


    }
}