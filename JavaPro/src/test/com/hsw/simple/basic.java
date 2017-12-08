package com.hsw.simple;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

/**
 * Created by hushiwei on 2017/3/29.
 */
public class basic {
    public static String sep = "\t";

    @Test
    public void sepStr() throws Exception {
        String line = "nice" + sep + "" + sep + "hello" + sep + "world";
        System.out.println(line.split(sep).length);
        System.out.println(line);

    }

    @Test
    public void testFastjson() throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("f1", "hello");
        jsonObject.put("f2", "world");
        String json = jsonObject.toJSONString();
        System.out.println(json);
        Map<String, String> parse = (Map<String, String>) JSON.parse(json);
        String f2 = parse.get("f3");
        System.out.println("---"+f2);

    }


    @Test
    public void continuDemo() throws Exception {
        for (int i = 0; i < 5; i++) {
            if (i != 3) {
                continue;
            } else {
                System.out.println(i);

            }
            System.out.println("hello world");
        }

    }

    @Test
    public void outDemo() throws Exception {
        ArrayList<String> list = new ArrayList<>();
        list.add("hello");
        list.add("world");
        list.add("hadoop");
        list.add("you");
        list.add("me");

        outter:for (String word : list) {

        }
    }

    @Test
    public void intern() throws Exception {
        String str = "helloworld";
        byte[] bytes = str.getBytes();
        System.out.println(bytes.length);
        String intern = str.intern();
        System.out.println(intern);
    }

    @Test
    public void sort() throws Exception {
        int[] arr={5,234,5653,2,234};
        ArrayList<Integer> arr2 = new ArrayList<>();
        Collections.sort(arr2);


    }
}
