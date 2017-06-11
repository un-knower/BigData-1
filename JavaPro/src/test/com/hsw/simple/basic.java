package com.hsw.simple;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.Map;

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
}
