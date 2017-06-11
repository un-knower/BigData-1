package com.jusfoun.bigdata.util.es;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * 查询条件的工具类 
 * @author admin
 *
 */
public class ESQueryUtil {
	/**
	 * 得到精确匹配的查询条件 
	 * @param key
	 * @param value
	 * @return
	 */
	public static QueryBuilder getTermQuery(String key , Object value){
		return QueryBuilders.termQuery(key , value ) ; 
	}

	/**
	 *  获得模糊查询的查询条件   
	 * @param key
	 * @param value 如 "*条件*"  查询所有key 带有 “条件” 的所有数据 
	 * @return
	 */
	public static QueryBuilder getWildcardQuery(String key , String value){
		return QueryBuilders.wildcardQuery(key , value ) ; 
	}
	/**
	 * 针对字段的  范围查询 ，整形 和日期都用这个方法 
	 * 日期类型 保存为 yyyyMMddHHmmss 的类型的长整形 
	 * @param key
	 * @param from
	 * @param to
	 * @return
	 */
	public static QueryBuilder getRangerQuery(String key , Integer from ,Integer to ){
		return QueryBuilders.rangeQuery(key).from(from).to( to ) ; 
	}
	
	/**
	 * 大于 小于的查询
	 * @param key
	 * @param from
	 * @param to
	 * @return
	 */
	public static QueryBuilder getRangerQuery2(String key , Integer from ,Integer to ){
		if(from ==null && to==null ){
			return null;  
		}
		RangeQueryBuilder rqb  = QueryBuilders.rangeQuery(key) ; //.from(from).to( to )
		if(from!=null){
			rqb.gt(from ) ; 
		}
		if(to!=null){
			rqb.lt( to ); 
		}	
		return rqb ;
	}
	
	/**
	 * 时间日期的范围查询 
	 * @param key
	 * @param from 大于
	 * @param to  小于
	 * @return
	 */
	public static QueryBuilder getRangerQuery2(String key , String from ,String to ){
		if(from ==null && to==null ){
			return null;  
		}
		RangeQueryBuilder rqb  = QueryBuilders.rangeQuery(key) ; //.from(from).to( to )
		if(from!=null && !"".equals( from )){
			rqb.gt(from ) ; 
		}
		if(to!=null && !"".equals( to )){
			rqb.lt( to ); 
		}	
		return rqb ;
	}
	
}
