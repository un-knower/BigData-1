package com.jusfoun.bigdata.util;

import java.util.Date;

import org.junit.Test;

import com.jusfoun.bigdata.util.es.JsonUtils;
/**
 * 测试json的工具类
 * @author admin
 *
 */
public class TestJsonUtils {

	@Test
	public void testToJson(){
		User user  = new User(); 
		user.setAge(20);
		user.setId(null);
		user.setName("wangzhantao"); 
		user.setTcreateDate(new Date());
		String userStr = JsonUtils.toJson(user) ;
		System.out.println( userStr);
		
		User user2 = JsonUtils.toObject( userStr, User.class) ;
		System.out.println( user2 );
	}
}
