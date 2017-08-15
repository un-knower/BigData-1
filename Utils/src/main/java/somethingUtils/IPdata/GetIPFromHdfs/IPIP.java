/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package somethingUtils.IPdata.GetIPFromHdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从HDFS上读取ip库
 */
public final class IPIP {

    private static final Log                             log     = LogFactory.getLog(IPIP.class);
    public static        ConcurrentHashMap<String, Data> IPCACHE = new ConcurrentHashMap<>();

    public static IPIP init() {
        Long st = System.nanoTime();
        IPIP ip = new IPIP();
        Long et = System.nanoTime();
        log.info((et - st) / 1000 / 1000);
        return ip;
    }

    private int offset;
    public static byte[] IPDB_FILE_CONTENT = null;
    private final int[]  index             = new int[256];
    private ByteBuffer dataBuffer;
    private ByteBuffer indexBuffer;
    private final ReentrantLock lock = new ReentrantLock();

    public static boolean isboolIp(String ipAddress) {
        String  ip      = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }

    public IPIP() {
        if (IPDB_FILE_CONTENT == null) {
            IPDB_FILE_CONTENT = readHDFSFile("hdfs://master:8020/user/hsw/ipdb.dat");
        }
        loadToTree();
    }

    /* 
     * read the hdfs file content 
     * notice that the dst is the full path name 
     */
    public byte[] readHDFSFile(String dst) {
        try {
            Configuration conf = new Configuration();
            FileSystem    fs   = FileSystem.get(conf);
            // check if the file exists
            Path path = new Path(dst);
            if (fs.exists(path)) {
                byte[] buffer;
                // get the file info to create the buffer
                try (FSDataInputStream is = fs.open(path)) {
                    // get the file info to create the buffer
                    FileStatus stat = fs.getFileStatus(path);
                    // create the buffer
                    buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                    is.readFully(0, buffer);
                }
                return buffer;
            } else {
                log.info("the file is not found .");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String[] find(String ip) {
        int  ip_prefix_value = new Integer(ip.substring(0, ip.indexOf(".")));
        long ip2long_value   = ip2long(ip);
        int  start           = index[ip_prefix_value];
        int  max_comp_len    = offset - 1028;
        long index_offset    = -1;
        int  index_length    = -1;
        byte b               = 0;
        for (start = start * 8 + 1024; start < max_comp_len; start += 8) {
            if (int2long(indexBuffer.getInt(start)) >= ip2long_value) {
                index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                index_length = 0xFF & indexBuffer.get(start + 7);
                break;
            }
        }
        byte[] areaBytes;
        lock.lock();
        try {
            dataBuffer.position(offset + (int) index_offset - 1024);
            areaBytes = new byte[index_length];
            dataBuffer.get(areaBytes, 0, index_length);
        } finally {
            lock.unlock();
        }
        return new String(areaBytes, Charset.forName("UTF-8")).split("\t", -1);
    }

    public Data getData(String ip) {
        Data data = null;
        if (IPCACHE.containsKey(ip)) {
            data = IPCACHE.get(ip);
        } else if (ip.contains(".")) {
            String dd = "";
            lock.lock();
            try {
                int  ip_prefix_value = new Integer(ip.substring(0, ip.indexOf(".")));
                long ip2long_value   = ip2long(ip);
                int  start           = index[ip_prefix_value];
                int  max_comp_len    = offset - 1028;
                long index_offset    = -1;
                int  index_length    = -1;
                byte b               = 0;
                for (start = start * 8 + 1024; start < max_comp_len; start += 8) {
                    if (int2long(indexBuffer.getInt(start)) >= ip2long_value) {
                        index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                        index_length = 0xFF & indexBuffer.get(start + 7);
                        break;
                    }
                }
                dataBuffer.position(offset + (int) index_offset - 1024);
                byte[] areaBytes = new byte[index_length];
                dataBuffer.get(areaBytes, 0, index_length);
                dd = new String(areaBytes, Charset.forName("UTF-8"));
            } finally {
                lock.unlock();
            }
            String[] datArr = dd.split("\t", -1);
            data = new Data(datArr[0], datArr[1], datArr[2], datArr[3]);
            IPCACHE.put(ip, data);
        } else {
            return new Data();
        }
        return data;
    }

    private void loadToTree() {
        FileInputStream fin = null;
        lock.lock();
        try {
            if (IPDB_FILE_CONTENT == null) {
                IPDB_FILE_CONTENT = readHDFSFile("hdfs://master:8020/user/hsw/ipdb.dat");
            }
            if (IPDB_FILE_CONTENT != null) {
                dataBuffer = ByteBuffer.allocate(Long.valueOf(IPDB_FILE_CONTENT.length).intValue());
                dataBuffer.put(IPDB_FILE_CONTENT);
                dataBuffer.position(0);
                int    indexLength = dataBuffer.getInt();
                byte[] indexBytes  = new byte[indexLength];
                dataBuffer.get(indexBytes, 0, indexLength - 4);
                indexBuffer = ByteBuffer.wrap(indexBytes);
                indexBuffer.order(ByteOrder.LITTLE_ENDIAN);
                offset = indexLength;
                int loop = 0;
                while (loop++ < 256) {
                    index[loop - 1] = indexBuffer.getInt();
                }
                indexBuffer.order(ByteOrder.BIG_ENDIAN);
            }
        } finally {
            lock.unlock();
        }
    }

    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int      a, b, c, d;
        a = Integer.parseInt(ss[0]);
        b = Integer.parseInt(ss[1]);
        c = Integer.parseInt(ss[2]);
        d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private static long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }


    public static void main(String[] args) {

        IPIP IP = IPIP.init();

        System.out.println(Arrays.toString(IP.find("118.28.8.8")));
        System.out.println(Arrays.toString(IP.find("101.90.9.10")));
        System.out.println(Arrays.toString(IP.find("101.70.9.10")));
        System.out.println(Arrays.toString(IP.find("192.168.1.127")));

        System.out.println(IP.getData("101.70.9.10").getCity());
        System.out.println(IP.getData("101.70.9.10").getCountry());
        System.out.println(IP.getData("101.70.9.10").getProvince());
    }

}
