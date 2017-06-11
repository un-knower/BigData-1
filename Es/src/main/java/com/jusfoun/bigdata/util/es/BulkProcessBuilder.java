package com.jusfoun.bigdata.util.es;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

/**
 * @author  bulk 进行数据录入的一个封装 
 * @date 2014年2月10日 下午3:27:43
 *
 */
public class BulkProcessBuilder {
	
	private static Logger logger = Logger.getLogger(BulkProcessBuilder.class);
	
	private BulkProcessor bulkProcessor ;
	private ESUtilAdvance esutil ; 
	
	public BulkProcessBuilder(ESUtilAdvance esutil){
		this.esutil = esutil ; 
		bulkProcessor = esutil.createBulk() ; 
	}
	
	/**
	 * bulk中添加数据
	 * @param indexName
	 * @param type
	 * @param listMap
	 * @param keyId
	 */
	public void appendData(String indexName,String type ,List<Map<String,Object>> listMap, String keyId){
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
	//关闭bulk
	public void closeBulk(){
		esutil.closeBulk(bulkProcessor );
	}
}