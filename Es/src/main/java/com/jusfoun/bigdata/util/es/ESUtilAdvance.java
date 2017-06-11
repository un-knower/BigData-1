package com.jusfoun.bigdata.util.es;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author huangfox
 * @date 2014年2月10日 下午3:27:43
 *
 */
public class ESUtilAdvance extends ESUtil{
	
	private static Logger logger = Logger.getLogger(JsonUtils.class);
	
	public ESUtilAdvance(String clusterName, String ips, Integer port) {
		super(clusterName, ips, port);
	}
	
	/**
	 * f返回ES的客户端对象 
	 * @return
	 */
	public Client getClient(){
		return super.client ; 
	}
	
	/**
	 *  bulk拉去数据请使用， BulkProcessBuilder类 ，其中对下面3个函数做了封装 ，隔离ES API
	 * 1 创建bulk 细粒度的批量提交   BackoffPolicy 批量请求重试失败
	 * 批量导入 log4j级别不要设置成debug
	 * @param indexName
	 * @param type
	 * @param bulkSize  单位为MB，批量提交总大小，设置-1可禁用  5
	 * @param bulkActions 提交的批次量，默认是1000，设置-1可以禁用  
	 * @param flushInterval  提交时间间隔
	 * @param concurrentRequests  线程数
	 * @param listMap
	 */
	public BulkProcessor createBulk(){

		long bulkSize=5l ; 
		int bulkActions = 20 ; 
		long flushInterval = 3l ; 
		int concurrentRequests = 1 ; 
		 
		BulkProcessor bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener(){
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				throw new ElasticsearchException(failure);
			}
			
		}).setBulkActions(bulkActions).setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
				.setConcurrentRequests(concurrentRequests)
				.setBackoffPolicy(
			           BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(500), 3)) 
			        .build();
		return bulkProcessor ; 
	}
	
	/**
	 * 2 bulk中添加数据 
	 * @param bulkProcessor
	 * @param listMap
	 * @param keyId  list的map数据中可以作为id的数据的key值
	 * @param indexName
	 * @param type
	 */
	public void addBulkData(String indexName,String type , 
			BulkProcessor bulkProcessor ,List<Map<String,Object>> listMap, String keyId){
		IndexRequest  indexRequest = null;
		if(keyId!=null && !"".equals( keyId)){
			for(Map<String,?> currentMap : listMap){
				indexRequest = new IndexRequest(indexName, type,currentMap.get(keyId).toString());
				logger.log(Priority.DEBUG,  currentMap );
				indexRequest.source(currentMap);
				bulkProcessor.add(indexRequest);
			}
		}else{
			for(Map<String,?> currentMap : listMap){
				indexRequest = new IndexRequest(indexName, type);
				indexRequest.source(currentMap);
				bulkProcessor.add(indexRequest);
			}
		}
	}
	
	/**
	 * 3 关闭 和刷新bulk 
	 * @param bulkProcessor
	 */
	public void closeBulk( BulkProcessor bulkProcessor ){
		bulkProcessor.flush();//收尾刷新一次
		bulkProcessor.close();
	}

	/**
	 * 细粒度的批量提交   BackoffPolicy 批量请求重试失败
	 * @param indexName
	 * @param type
	 * @param bulkSize  单位为MB，批量提交总大小，设置-1可禁用  5
	 * @param bulkActions 提交的批次量，默认是1000，设置-1可以禁用  
	 * @param flushInterval  提交时间间隔
	 * @param concurrentRequests  线程数
	 * @param listMap
	 */
	@Deprecated
	public void bulkProcessorIndex(String indexName,String type,String keyId, List<Map<String,Object>> listMap){
		long bulkSize=5l ; 
		int bulkActions = 20 ; 
		long flushInterval = 3l ; 
		int concurrentRequests = 1 ; 
		 
		IndexRequest  indexRequest = null;
		BulkProcessor bulkProcessor = BulkProcessor.builder(getClient(), new BulkProcessor.Listener(){
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				throw new ElasticsearchException(failure);
			}
			
		}).setBulkActions(bulkActions).setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
				.setConcurrentRequests(concurrentRequests)
				.setBackoffPolicy(
			           BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(500), 3)) 
			        .build();
		
		if(keyId!=null && !"".equals( keyId)){
			for(Map<String,?> currentMap : listMap){
				indexRequest = new IndexRequest(indexName, type,currentMap.get(keyId).toString());
				logger.log(Priority.DEBUG,  currentMap );
				indexRequest.source(currentMap);
				bulkProcessor.add(indexRequest);
			}
		}else{
			for(Map<String,?> currentMap : listMap){
				indexRequest = new IndexRequest(indexName, type);
				indexRequest.source(currentMap);
				bulkProcessor.add(indexRequest);
			}
		}
		
		bulkProcessor.flush();//收尾刷新一次
		bulkProcessor.close();
	}
	
	/**
	 * 创建映射
	 * @param indices
	 * @param mappingType
	 * @param mappingSource
	 * @return
	 */
	public boolean createMapping(String indices, String mappingType , String mappingSource ){
		PutMappingRequest mapping = Requests.putMappingRequest(indices).type(mappingType).source(mappingSource);
		PutMappingResponse response = client.admin().indices().putMapping(mapping).actionGet();
		return response.isAcknowledged() ;
	}
	
	/**
	 * 创建映射的mapping 
	 * @param mappingType
	 * @param fieldMap
	 * @return
	 */
	public String createMappingSource(String mappingType , Map<String,Map<String,String>> fieldMap){
		new XContentFactory();
		XContentBuilder builder = null;
		try {
			builder = XContentFactory.jsonBuilder()
					.startObject()
					.startObject(mappingType)
					.startObject("properties"); 

			Set<String> setFieldKey = fieldMap.keySet() ;
			Iterator<String> it  = setFieldKey.iterator() ; 
			while(it.hasNext()){
				String key = it.next() ; 
				builder.startObject( key);
				Map<String,String> childMap = fieldMap.get(key) ; 
				Set<String> childSet = childMap.keySet() ;
				Iterator<String> childIt  = childSet.iterator() ; 
				while(childIt.hasNext()){
					String key1 = childIt.next() ; 
					String val1 = childMap.get(key1);
					if(key1!=null && "type".equals(key1)  &&  "date".equals(val1) ){//发现该字段是日期类型
						builder.field("type" , "date") ;
						builder.field("store" , "false") ;
						builder.field("index" , "not_analyzed") ;
						builder.field("format" , "yyyy-MM-dd HH:mm:ss") ;
						//builder.field("format" , "yyyy-MM-dd") ;
						break; 
					}
					if(key1!=null && "type".equals(key1) && "integer".equals( val1) ){//发现该字段是数字类型
						builder.field("type" , "integer") ;
						builder.field("store" , "false") ;
						builder.field("index" , "not_analyzed") ;
						break; 
					}	
					builder.field(key1 , val1) ; 
				}
				builder.endObject() ; 
			}
			
			builder.endObject() .endObject() .endObject();
			System.out.println(builder.string() );
			return  builder.string() ; 
		} catch (IOException e) {
			logger.error(e.getMessage() );
			e.printStackTrace();
		}
		return null ;
	}
	
	
	
}