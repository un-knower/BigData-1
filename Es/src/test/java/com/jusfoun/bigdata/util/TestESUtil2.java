package com.jusfoun.bigdata.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import com.jusfoun.bigdata.util.es.ESGroupCriteria;
import com.jusfoun.bigdata.util.es.ESQueryCriteria;
import com.jusfoun.bigdata.util.es.ESQueryUtil;
import com.jusfoun.bigdata.util.es.ESUtil;
import com.jusfoun.bigdata.util.es.ESUtilAdvance;
import com.jusfoun.bigdata.util.es.IndexEntity;
import com.jusfoun.bigdata.util.es.StringUtils;

/**
 * ES 工具类测试 
 * @author admin
 *
 */
public class TestESUtil2 {
	
	String index = "systemlog" ; 
	String type = "type1" ; 
//	User user ; 
	ESUtilAdvance esUtil ; 
 
	
	@Before
	public void initUser(){
		esUtil = new ESUtilAdvance("es-ncp-cluster1" , "192.168.4.203,192.168.4.205,192.168.4.206",9300 ); 
		 
	}
 

 
	
	//测试 条件为or查询
	@Test
	public void testQueryAndCriteria(){
		ESQueryCriteria criteria= new ESQueryCriteria() ; 
		criteria.setIndex(index);
		criteria.setType(type);
		criteria.setPageSize(100);
		//criteria.setCurPage(1);
		List<QueryBuilder> listQuery = new ArrayList<QueryBuilder>(); 
		listQuery.add(ESQueryUtil.getRangerQuery2("createdate", "2016-07-15 09:44:16", "2016-07-16 00:00:00") );
		
		List<Map<String,Object>> list = esUtil.queryOrCriteria( criteria,listQuery);
		if(list!=null && list.size()>0 ){
			System.out.println( list.size() );
			for(Map<String,Object> map: list){
				System.out.println( map );
			}
		}
	}
	 
}
