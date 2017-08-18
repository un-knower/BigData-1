package com.hsw.jdk;

import java.util.ArrayList;

/**
 * Created by hushiwei on 2017/8/7.
 *
 * 以64kb/50毫秒的速度往java堆中填充数据,一共填充1000次
 */
public class JConsoleDemo {

    /**
     * 内存占位符对象,一个OOMObject 大约占64k
     */
    static class OOMObject{
        public byte[] placeholder = new byte[64 * 1024];
    }

    public static void fillHeap(int num) throws InterruptedException {
        ArrayList<OOMObject> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            Thread.sleep(50);
            list.add(new OOMObject());
        }
        System.gc();
    }

    public static void main(String[] args) throws InterruptedException {
        fillHeap(1000);
    }
}
