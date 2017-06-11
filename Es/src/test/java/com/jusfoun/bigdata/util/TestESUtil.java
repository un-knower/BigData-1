package com.jusfoun.bigdata.util;

import java.util.ArrayList;
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
import com.jusfoun.bigdata.util.es.ESUtilAdvance;
import com.jusfoun.bigdata.util.es.IndexEntity;
import com.jusfoun.bigdata.util.es.StringUtils;



/**
 * ES 工具类测试 
 * @author admin
 *
 */
public class TestESUtil {
	
	String index = "LOGGERlogger8" ; 
	String type = "icecallloger" ; 
//	User user ; 
	ESUtilAdvance esUtil ; 
	String msg = "INFO|20160415113545|getSerListInfo|colonySerList|192.168.4.213|192.168.4.213|6|success" ; 
	ICECallLog log ; 
	
	@Before
	public void initUser(){
		esUtil = new ESUtilAdvance("es-ncp-cluster1" , "192.168.4.203,192.168.4.205,192.168.4.206",9300 ); 
		log = getICECallLogEntity(msg);
	}

	private  ICECallLog getICECallLogEntity(String content) {
		if(StringUtils.isEmpty(content )){
			return null ; 
		}
		String[] strs = content.split("\\|") ; 
		if(strs==null || strs.length!=8){
			return null ; 
		}
		if(strs[1].trim().length()!=14){
			System.out.println("=="+ strs[1].trim()+"==");
			return null ; 
		}
		ICECallLog loggerEntity;
		try {
			loggerEntity = new ICECallLog(); 

			loggerEntity.setCalltime(Long.parseLong(strs[1]));
			loggerEntity.setMethodName( strs[2]  );
			loggerEntity.setServiceName( strs[3] );
			loggerEntity.setServerIp( strs[4] );
			loggerEntity.setClientIp(  strs[5] );
			loggerEntity.setCallLastTime( Long.parseLong(strs[6]));
			loggerEntity.setSucessOrFail( strs[7]);
			//System.out.println( JsonUtils.toStr(loggerEntity).toLowerCase() ); 
			return loggerEntity;
		} catch (NumberFormatException e) {
			return null ;
		}
	}

	//创建索引 
	@Test
	public void testCreateIndex(){
		boolean isSucess = esUtil.createIndex(index ) ;
		esUtil.close(); 
		System.out.println( "创建索引是否成功== "+isSucess); 
	}

	//创建索引 

//	 		XContentBuilder builder = XContentFactory.jsonBuilder()
//				.startObject()
//				.startObject(mappingType)
//				.startObject("properties")
//				.startObject("id").field("type", "integer").field("store", "yes").field("index", "analyzed").endObject()
//				.startObject("kw").field("type", "string").field("store", "yes").field("index", "analyzed").field("analyzer", "ik").endObject()
//				.startObject("edate").field("type", "date").field("store", "yes").endObject() ; 
	@Test
	public void testCreateMapping(){
		
		Map<String,Map<String,String>> fieldMap = new HashMap<String,Map<String,String>>();
		
//		Map<String,String> idMap = new HashMap<String,String>();
//		idMap.put("type", "integer") ; 
//		idMap.put("store", "no") ; 
//		idMap.put("index", "not_analyzed") ; 
//		//idMap.put("analyzer", "analyzed") ; 
//
//		Map<String,String> kwMap = new HashMap<String,String>();
//		kwMap.put("type", "string") ; 
//		kwMap.put("store", "yes") ; 
//		kwMap.put("index", "analyzed") ; 
//		kwMap.put("analyzer", "ik") ; 
//
//		Map<String,String> dateMap = new HashMap<String,String>();
//		dateMap.put("type", "date") ; 
//		dateMap.put("store", "yes") ; 
//		//dateMap.put("index", "analyzed") ; 
//		//dateMap.put("analyzer", "ik") ; 
		
		Map<String,String> calltimeMap = new HashMap<String,String>();
		calltimeMap.put("type", "long") ; 
		calltimeMap.put("store", "no") ; 
		calltimeMap.put("index", "not_analyzed") ; 
		fieldMap.put("calltime", calltimeMap) ;
		
		Map<String,String> methodNameMap = new HashMap<String,String>();
		methodNameMap.put("type", "string") ; 
		methodNameMap.put("store", "yes") ; 
		methodNameMap.put("index", "not_analyzed") ; 
		fieldMap.put("methodName", methodNameMap) ;
		

		Map<String,String> serviceNameMap = new HashMap<String,String>();
		serviceNameMap.put("type", "string") ; 
		serviceNameMap.put("store", "yes") ; 
		serviceNameMap.put("index", "not_analyzed") ; 
		//serviceNameMap.put("analyzer", "ik") ; 
		fieldMap.put("serviceName", serviceNameMap) ;

		Map<String,String> serverIpMap = new HashMap<String,String>();
		serverIpMap.put("type", "string") ; 
		serverIpMap.put("store", "yes") ; 
		serverIpMap.put("index", "not_analyzed") ; 
		fieldMap.put("serverIp", serverIpMap) ;
		
		Map<String,String> clientIpMap = new HashMap<String,String>();
		clientIpMap.put("type", "string") ; 
		clientIpMap.put("store", "yes") ; 
		clientIpMap.put("index", "not_analyzed") ; 
		fieldMap.put("clientIp", clientIpMap) ;
		
		Map<String,String> callLastTimeMap = new HashMap<String,String>();
		callLastTimeMap.put("type", "integer") ; 
		callLastTimeMap.put("store", "yes") ; 
		callLastTimeMap.put("index", "not_analyzed") ;
		fieldMap.put("callLastTime", callLastTimeMap) ;
		
		
		Map<String,String> sucessOrFailMap = new HashMap<String,String>();
		sucessOrFailMap.put("type", "string") ; 
		sucessOrFailMap.put("store", "yes") ; 
		sucessOrFailMap.put("index", "not_analyzed") ;
		fieldMap.put("sucessOrFail", sucessOrFailMap) ;

		Map<String,String> calltimeYearMap = new HashMap<String,String>();
		calltimeYearMap.put("type", "integer") ; 
		calltimeYearMap.put("store", "no") ; 
		calltimeYearMap.put("index", "not_analyzed") ;
		fieldMap.put("calltimeYear", calltimeYearMap) ;
		
		
		Map<String,String> calltimeMonthMap = new HashMap<String,String>();
		calltimeMonthMap.put("type", "integer") ; 
		calltimeMonthMap.put("store", "no") ; 
		calltimeMonthMap.put("index", "not_analyzed") ;
		fieldMap.put("calltimeMonth", calltimeMonthMap) ;

		Map<String,String> calltimeDayMap = new HashMap<String,String>();
		calltimeDayMap.put("type", "integer") ; 
		calltimeDayMap.put("store", "no") ; 
		calltimeDayMap.put("index", "not_analyzed") ;
		fieldMap.put("calltimeDay", calltimeDayMap) ;
		
		Map<String,String> calltimeHourMap = new HashMap<String,String>();
		calltimeHourMap.put("type", "integer") ; 
		calltimeHourMap.put("store", "no") ; 
		calltimeHourMap.put("index", "not_analyzed") ;
		fieldMap.put("calltimeHour", calltimeHourMap) ;

		Map<String,String> calltimeMinuteMap = new HashMap<String,String>();
		calltimeMinuteMap.put("type", "integer") ; 
		calltimeMinuteMap.put("store", "no") ; 
		calltimeMinuteMap.put("index", "not_analyzed") ;		
		fieldMap.put("calltimeMinute", calltimeMinuteMap) ;

		Map<String,String> calltimeSecondMap = new HashMap<String,String>();
		calltimeSecondMap.put("type", "integer") ; 
		calltimeSecondMap.put("store", "no") ; 
		calltimeSecondMap.put("index", "not_analyzed") ;	
		fieldMap.put("calltimeSecond", calltimeSecondMap) ;
		
		boolean isSucess = esUtil.createMapping(index, type, fieldMap) ;
		esUtil.close(); 
		System.out.println( "创建mapping是否成功= "+isSucess); 
	}
	
	//增加索引 
//	@Test
//	public void testCreateIndex(){
//		String id = esUtil.createIndex(index, type , log) ;
//		esUtil.close(); 
//		System.out.println( "索引id= "+id); 
//		
//	}

	//增加数据 
	@Test
	public void testInsertData(){
		
		for(int i=0;i<5;i++){
			log.setMethodName( log.getMethodName()+i );
			String id2 = esUtil.insertData(index,type,log ) ;
			System.out.println( "索引id= " + id2 ); 
		}
		esUtil.close(); 
	}

	@Test
	public void testInsertData2(){
		List<Map<String,Object>> listMap = new ArrayList<Map<String,Object>>();
		Map map = new HashMap();
		map.put("COUNTRYCODE", "1" ) ; 
		map.put("ID", "43") ; 
		map.put("COUNTRYNAME",  "COUNTRYNAME1") ;
		map.put("CREATETIME",  "2016-03-31 22:09:42") ;
		listMap.add( map) ; 
		//esUtil.insertData("jusfoun","COUNTRY_TEST2",  map) ; 
		//esUtil.insertData("jusfoun","COUNTRY_TEST2",map) ;
//		esUtil.bulkProcessorIndex("jusfoun","COUNTRY_TEST2", "ID", listMap);
		esUtil.close(); 
	}

	@Test
	public void testInsertData3(){
		List<Map<String,Object>> listMap = new ArrayList<Map<String,Object>>();
		Map map = new HashMap();
		map.put("date",  "2016-03-31 22:09:42") ;
		listMap.add( map) ; 
		
//		esUtil.insertData("my_index","my_type",map) ;
//		esUtil.bulkProcessorIndex("my_index","my_type", "date", listMap);
		esUtil.close(); 
	}
	
	//添加数据  主从格式
	@Test
	public void testZhuCongInsertData(){
		UUID uuid = UUID.randomUUID();
		IndexEntity indexEntity = new IndexEntity(); 
		indexEntity.setIndex( "zhu_cong"  );
		indexEntity.setId( uuid.toString()  );
		indexEntity.setType( "cong");
		indexEntity.setParentId("AVSKLc6nPEGuNfxrgIzo" );
		
		Cong cong  = new Cong(); 
		cong.setPatYear("2016");
		cong.setProvinceCn("shanghai"); 

		Cong cong2  = new Cong(); 
		cong2.setPatYear("2017");
		cong2.setProvinceCn("shanghai2"); 
		
		String id = esUtil.insertData(indexEntity,cong  ) ;
		UUID uuid2 = UUID.randomUUID();
		
		indexEntity.setId( uuid2.toString()  );
		String id2 = esUtil.insertData(indexEntity,cong2  ) ;
		
		
		esUtil.close(); 
		System.out.println( "索引id= "+id +"   "+ id2 ); 
		
	}
	
	//测试 条件为and查询 
	@Test
	public void testQueryAndCriteria(){
		Long start = System.currentTimeMillis();
		System.out.println( "开始="+start );
		ESQueryCriteria criteria= new ESQueryCriteria() ; 
		criteria.setIndex(index);
		criteria.setType("type1");
//		criteria.setPageSize(2);
//		criteria.setCurPage(3);
		
		criteria.setOrderName("methodName");
		criteria.setSortOrder(SortOrder.DESC);
		
		List<QueryBuilder> listQuery = new ArrayList<QueryBuilder>(); 
//		listQuery.add(ESQueryUtil.getWildcardQuery("methodName", "*getserlist*" ) );
		listQuery.add(ESQueryUtil.getTermQuery("methodName", "getserlistinfo0" ) );
		
		 
		List<Map<String,Object>> list = esUtil.queryAndCriteria(criteria,listQuery);
		System.out.println( "结束="+start );
		System.out.println((System.currentTimeMillis()-start)/1000 );
		if(list!=null && list.size()>0 ){
			for(Map<String,Object> map: list){
				System.out.println( map +" == ");
			}
		}
		esUtil.close(); 
	}
	
	//测试 条件为or查询
	@Test
	public void testQueryOrCriteria(){
		ESQueryCriteria criteria= new ESQueryCriteria() ; 
		criteria.setIndex(index);
		criteria.setType(type);
//		criteria.setPageSize(2);
//		criteria.setCurPage(1);

		List<QueryBuilder> listQuery = new ArrayList<QueryBuilder>(); 
//		listQuery.add(ESQueryUtil.getWildcardQuery("name", "*wangzhantao*" ) );
//		listQuery.add(ESQueryUtil.getTermQuery( "name", "wangzhantao0" ) );
		listQuery.add(ESQueryUtil.getWildcardQuery( "methodName", "getserlistinfo01*" ) );
		listQuery.add(ESQueryUtil.getTermQuery( "serviceName", "colonyserlist" ) );
//		listQuery.add(ESQueryUtil.getRangerQuery("age", 5, 30) );
		
		List<Map<String,Object>> list = esUtil.queryOrCriteria( criteria,listQuery);
		if(list!=null && list.size()>0 ){
			for(Map<String,Object> map: list){
				System.out.println( map );
			}
		}
	}
	//查询总数
	@Test
	public void testGetCount(){
		ESQueryCriteria criteria= new ESQueryCriteria() ; 
		index="jusfoun";
		type="COUNTRY_TEST2" ; 
		criteria.setIndex(index);
		criteria.setType(type);
//		criteria.setPageSize(1);
//		criteria.setCurPage(3);
		List<QueryBuilder> listQuery = new ArrayList<QueryBuilder>(); 
//		listQuery.add(ESQueryUtil.getWildcardQuery("name", "*wangzhantao*" ) );
//		listQuery.add(ESQueryUtil.getTermQuery( "name", "wangzhantao0" ) );
//		listQuery.add(ESQueryUtil.getRangerQuery("calltimeMonth", 2, 4) );
		Long count = esUtil.getCount(criteria,listQuery );
		System.out.println( count ); 
	}
	
	@Test
	public void testExecuteGroupCount(){
		//设置查询的 数据库 和表 
		ESQueryCriteria criteria= new ESQueryCriteria() ; 
		criteria.setIndex(index);
		criteria.setType(type);

		List<QueryBuilder> listQuery = new ArrayList<QueryBuilder>(); 
		// 设置过滤条件 年月日 过滤条件 
		QueryBuilder year = QueryBuilders.termQuery( "calltimeYear", 2016) ;
		QueryBuilder month = QueryBuilders.termQuery( "calltimeMonth", 4) ;
		QueryBuilder day = QueryBuilders.termQuery( "calltimeDay", 15) ; 
		listQuery.add( year);
		listQuery.add( month);
		listQuery.add(day ) ;
		
		//设置分组条件 
		ESGroupCriteria esGroupCriteria = new ESGroupCriteria();
		esGroupCriteria.setGroupField("sucessOrFail");
		
		List<ESGroupCriteria> list = esUtil.executeGroupCount(criteria, listQuery, esGroupCriteria) ;
		for(ESGroupCriteria a : list  ){
			System.out.println( a );
		}
	}
	
	@Test
	public void testGetCompletionSuggest(){
		List list = esUtil.getCompletionSuggest( "music","suggest" ,"永", 10 );
		System.out.println( list );
		esUtil.close(); 
	}
}
