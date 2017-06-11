package com.hushiwei.flume.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 * 压缩解压字符串工具类
 */
public class CompressUtil {
    public static byte[] compress(String message)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream dos = new DeflaterOutputStream(baos);
        dos.write(message.getBytes("UTF-8"));
        dos.close();
        return baos.toByteArray();
    }

    public static String uncompress(byte[] message)
            throws IOException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(message);
        InflaterInputStream ii = new InflaterInputStream(bis);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int c = 0;
        byte[] buf = new byte[2048];
        while (true) {
            c = ii.read(buf);
            if (c == -1)
                break;
            baos.write(buf, 0, c);
        }
        ii.close();
        baos.close();
        String res = new String(baos.toByteArray(), "UTF-8");
        return res;
    }
}
