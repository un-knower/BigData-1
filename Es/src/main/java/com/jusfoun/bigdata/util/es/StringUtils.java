package com.jusfoun.bigdata.util.es;

import java.util.Arrays;
import java.util.List;

/**
 * 字符串工具类
 * @author admin
 *
 */
public class StringUtils {
	
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 判断为非空
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }
    
    public static List<String> splitStr( String str , String splitChar ){
    	if(isEmpty(str) ){
    		return null; 
    	}
    	return  Arrays.asList( str.split(splitChar) )  ;
    }
}
