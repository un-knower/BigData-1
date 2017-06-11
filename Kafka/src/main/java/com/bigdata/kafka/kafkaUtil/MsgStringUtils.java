package com.bigdata.kafka.kafkaUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * 字符串工具类
 * @author admin
 *
 */
public class MsgStringUtils {
	
	//字符串判断空 
	public static  boolean isNull(String str){
		if(str==null || "".equals( str)){
			return true ;
		}else{
			return false ; 
		}
	}
	//字符串转换为Set集合
	public static Set<String> arrToSet( String[] strs ){
		if(strs==null || strs.length==0){
			return null; 
		}
		Set<String> set = new HashSet<String>();
		for(String str :strs ){
			set.add( str) ; 
		}
		return set ; 
	}
}
