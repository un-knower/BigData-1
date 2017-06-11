package com.jusfoun.bigdata.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 * @author admin
 *
 */
public class DateUtils {

	 private static final SimpleDateFormat datetimeFormat = new SimpleDateFormat(  
	            "yyyy-MM-dd HH:mm:ss");  
	 
	 
    /** 
     * 根据自定义pattern将字符串日期转换成java.util.Date类型 
     *  
     * @param datetime 
     * @param pattern 
     * @return 
     * @throws ParseException 
     */  
    public static Date parseDatetime(String datetime, String pattern)  
            throws ParseException {  
        SimpleDateFormat format = (SimpleDateFormat) datetimeFormat.clone();  
        format.applyPattern(pattern);  
        return format.parse(datetime);  
    }
    
}
