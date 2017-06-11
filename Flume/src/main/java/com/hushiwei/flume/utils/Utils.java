package com.hushiwei.flume.utils;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
public class Utils {
    public static Map<String, String> processMessageToMap(String message)
    {
        if (message.startsWith("{")) {
            message = message.substring(1);
        }
        if (message.endsWith("}")) {
            message = message.substring(0, message.length() - 1);
        }

        Map map = new HashMap();
        String[] keyValues = message.split(",");
        for (String kv : keyValues) {
            String[] fields = kv.split(":");
            if (fields.length == 1)
                map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), "");
            else {
                map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), StringUtils.trim(fields[1]).replaceAll("\"", ""));
            }
        }
        return map;
    }

    public static String getTimePath(String time)
            throws ParseException
    {
        SimpleDateFormat dirSdf = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat timeSdf = new SimpleDateFormat("yyyyMMddHHmmss");
        if (time.length() == "150714140400".length()) {
            time = "20" + time;
        }
        String dir = dirSdf.format(timeSdf.parse(time));
        return dir;
    }

    public static long convertToUTC(String time)
            throws ParseException
    {
        if (time.length() == "150714140400".length()) {
            time = "20" + time;
        }
        SimpleDateFormat timeSdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return timeSdf.parse(time).getTime();
    }
}
