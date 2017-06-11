package com.jusfoun.bigdata.util.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

/**
 * @author huangfox
 * @date 2014年2月10日 下午3:27:43
 *
 */
public class ESUtil {
	
	 private static Logger logger = Logger.getLogger(JsonUtils.class);
	 protected  Client client;

//	static{
//		//集群设置
//		 Settings settings = Settings.settingsBuilder()
//		 .put("cluster.name", "es_prod")
//		 .put("client.transport.sniff", true)
//		 .put("client.transport.ping_timeout", "5s")
//		 .build();
//		try {
//			client = TransportClient.builder().settings(settings).build()
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.4.207"), 9300));
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//	}

	public ESUtil(String clusterName ,String ips,Integer port  ){
		 //集群设置
		 Settings settings = Settings.settingsBuilder()
		 .put("cluster.name", clusterName)
		 .put("client.transport.sniff", true)
		 .put("client.transport.ping_timeout", "5s")
		 .build();
		 
		 List<String> listAddr =  StringUtils.splitStr(ips,"," );
		 TransportClient transportClient = TransportClient.builder().settings(settings).build();
		 try {
			for(int i=0;i<listAddr.size();i++ ){
				 client =  transportClient.addTransportAddress(
						 new InetSocketTransportAddress(InetAddress.getByName(listAddr.get(i) ), port)); 
			 }
		} catch (UnknownHostException e) {
			//e.printStackTrace();
			logger.error( e.getMessage()); 
		}
	}
	
	//删除索引数据 ,根据数据的 index  type 和数据id 
	public  String delIndex(String index, String type, String id){
		DeleteResponse response_del = client.prepareDelete(index, type,  id ).get();
		return response_del.getIndex() ; 
	}
 
	
	public  void close() {
		client.close();
	}


	/**
	 * 创建索引，返回插入数据的id 
	 * obj 为类的对象 
	 */
	public  boolean createIndex(String indices ) {
		CreateIndexResponse response =  client.admin().indices().prepareCreate(indices).execute().actionGet();
		return response.isAcknowledged() ; 
	}
	
	/**
	 * 		XContentBuilder builder = XContentFactory.jsonBuilder()
				.startObject()
				.startObject(mappingType)
				.startObject("properties")
				.startObject("id").field("type", "integer").field("store", "yes").field("index", "analyzed").endObject()
				.startObject("kw").field("type", "string").field("store", "yes").field("index", "analyzed").field("analyzer", "ik").endObject()
				.startObject("edate").field("type", "date").field("store", "yes").endObject() ; 
	 * @param indices
	 * @param mappingType
	 * @param fieldMap
	 * @return
	 * @throws IOException
	 */
	public boolean createMapping(String indices, String mappingType , Map<String,Map<String,String>> fieldMap){
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
					builder.field(key1 , val1) ; 
				}
				builder.endObject() ; 
			}
			
			builder.endObject() .endObject() .endObject();
			System.out.println(builder.string() );
 
		} catch (IOException e) {
			logger.error(e.getMessage() );
			e.printStackTrace();
		}

		PutMappingRequest mapping = Requests.putMappingRequest(indices).type(mappingType).source(builder);
		PutMappingResponse response = client.admin().indices().putMapping(mapping).actionGet();
		return response.isAcknowledged() ;
	}
	
	
	
	/**
	 * 创建索引，返回插入数据的id 
	 * obj 为类的对象 
	 */
	@Deprecated
	public  String createIndex(String index,String type,Object obj ) {
		//IndexResponse response  =  client.prepareIndex(index, type ).setSource(JsonUtils.toBytes( obj )).execute().actionGet();
		IndexResponse response  =  client.prepareIndex(index, type ).setSource(JsonUtils.toStr(obj).toLowerCase()).execute().actionGet();
		return response.getId(); 
	}

	//直接插入数据
	public  String insertData(String index,String type,Object obj ) {
		IndexResponse response  =  client.prepareIndex(index, type ).setSource(JsonUtils.toStr(obj)).execute().actionGet();
		return response.getId(); 
	}
	//需要传入数据，索引id 
	public  String insertData(String index,String type,String id,Object obj ) {
		IndexResponse response  =  client.prepareIndex(index, type,id ).setSource(JsonUtils.toStr(obj)).execute().actionGet();
		return response.getId(); 
	}
	
	/**
	 * 
	 * @param index  插入那个索引  ，必选
	 * @param type  那个表    必选
	 * @param obj obj 为类的对象 或者 map对象 必选  
	 * @param id  制定插入数据的id  可选 
	 * @param parentId 制定父id   可选，父子关系则必选
	 * @return创建索引，返回插入数据的id  
	 */
	public  String insertData( IndexEntity indexEntity ,Object obj   ) {
		//IndexResponse response  =  client.prepareIndex(index, type ).setSource(JsonUtils.toBytes( obj )).execute().actionGet();
		IndexRequestBuilder irb = client.prepareIndex();
		irb.setIndex(indexEntity.getIndex() );
		if(indexEntity!=null && indexEntity.getType()!=null && StringUtils.isNotEmpty( indexEntity.getType())){
			irb.setType( indexEntity.getType( )) ; 
		}
		if(indexEntity!=null && indexEntity.getId()!=null && StringUtils.isNotEmpty( indexEntity.getId())){
			irb.setId( indexEntity.getId() ) ; 
		}	
		if(indexEntity!=null && indexEntity.getParentId()!=null && StringUtils.isNotEmpty( indexEntity.getParentId())){
			irb.setParent(indexEntity.getParentId() ) ; 
		}	
		
		IndexResponse response  =  irb.setSource(JsonUtils.toStr(obj)).execute().actionGet();
		return response.getId(); 
	}
	
	/**
	 * 分组统计数量 
	 * @param criteria 查询需要的索引和 type 
	 * @param listQuery 过滤条件列表 
	 * groupCri  分组统计个数
	 * @return
	 */
	public  List<ESGroupCriteria> executeGroupCount(ESQueryCriteria criteria , 
			List<QueryBuilder> listQuery , ESGroupCriteria groupCri){
		if(criteria==null){
			return null; 
		}
		SearchRequestBuilder srb =  client.prepareSearch().setIndices( criteria.getIndex())
				.setSearchType(SearchType.QUERY_THEN_FETCH  ); 
		if( StringUtils.isNotEmpty( criteria.getType()) ){
			srb.setTypes( criteria.getType() ) ; 
		}
		if( listQuery!=null && listQuery.size()>0){
			//and 查询 
			BoolQueryBuilder blQuery = QueryBuilders.boolQuery();
			for( QueryBuilder queryBuilderT : listQuery){
				blQuery.must( queryBuilderT )  ; 
			}
			srb.setQuery( blQuery ) ; 
		}else{
			srb.setQuery( QueryBuilders.matchAllQuery() ) ; 
		}
		//设置分组查询
		srb.addAggregation(AggregationBuilders.terms("by_"+groupCri.getGroupField())
				.field( groupCri.getGroupField()).size(Integer.MAX_VALUE) ) ;
		
		SearchResponse response =srb.execute().actionGet();
		Terms genders =   response.getAggregations().get( "by_"+groupCri.getGroupField());
		
		List<ESGroupCriteria> listESGroupCriteria = new ArrayList<ESGroupCriteria>(); 
		ESGroupCriteria esGroupCriteria  ; 
		for(Terms.Bucket entry : genders.getBuckets()){
			 esGroupCriteria = new ESGroupCriteria() ;
			 esGroupCriteria.setFieldValue( entry.getKey());
			 esGroupCriteria.setFieldCount( entry.getDocCount());
			 listESGroupCriteria.add(esGroupCriteria ) ; 
		}
		
		return   listESGroupCriteria ; 
	}
	
	/**
	 * 查询条件为 组合 为  and 
	 * @param criteria 索引  排序   分页 的条件 
	 * @param listQuery 查询的条件 列表 
	 * @return
	 */
	private  List<Map<String,Object>> executeAndQuery(ESQueryCriteria criteria , List<QueryBuilder> listQuery){
		 
		SearchRequestBuilder srb =  client.prepareSearch().setIndices( criteria.getIndex())
				.setSearchType(SearchType.QUERY_THEN_FETCH  ); 
		if(StringUtils.isNotEmpty( criteria.getType())){
			srb.setTypes( criteria.getType() ) ; 
		}
		if( listQuery!=null && listQuery.size()>0){
			//and 查询 
			BoolQueryBuilder blQuery = QueryBuilders.boolQuery();
			for( QueryBuilder queryBuilderT : listQuery){
				blQuery.must( queryBuilderT )  ; 
			}
			srb.setQuery( blQuery ) ; 
		}else{
			srb.setQuery( QueryBuilders.matchAllQuery() ) ; 
		}
		
		if(StringUtils.isNotEmpty(criteria.getOrderName() ) ){
			srb.addSort(criteria.getOrderName()  , criteria.getSortOrder()) ; //设置排序 
		}
		if(criteria.getCurPage()!=null && criteria.getStartRow()!=null ){
			srb.setFrom(criteria.getStartRow() ).setSize(criteria.getPageSize()  ) ; //设置排序 
		}	
		SearchResponse response =srb.execute().actionGet();
		SearchHits hits = response.getHits();
		 
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (int i = 0; i < hits.getHits().length; i++) {
			Map<String,Object> map = JsonUtils.strToMap(hits.getHits()[i].getSourceAsString() );
			map.put("es_id", hits.getHits()[i].getId()  ) ;  
			map.put("es_index", hits.getHits()[i].getIndex()  ) ;
			map.put("es_type", hits.getHits()[i].getType() ) ; 
			list.add( map); 
			//System.out.println(hits.getHits()[i].getSourceAsString());
			logger.debug( hits.getHits()[i].getSourceAsString());
		}
		return list ; 
	}
	/**
	 * 得到查询总数 
	 * @param criteria
	 * @param listQuery 条件是and查询
	 * @return
	 */
	public  Long executeCount(ESQueryCriteria criteria , List<QueryBuilder> listQuery){
		 
		SearchRequestBuilder srb =  client.prepareSearch().setIndices( criteria.getIndex())
				.setSearchType(SearchType.COUNT  ); 
		if(StringUtils.isNotEmpty( criteria.getType())){
			srb.setTypes( criteria.getType() ) ; 
		}
		if( listQuery!=null && listQuery.size()>0){
			//and 查询 
			BoolQueryBuilder blQuery = QueryBuilders.boolQuery();
			for( QueryBuilder queryBuilderT : listQuery){
				blQuery.must( queryBuilderT )  ; 
			}
			srb.setQuery( blQuery ) ; 
		}else{
			srb.setQuery( QueryBuilders.matchAllQuery() ) ; 
		}
		
		if(StringUtils.isNotEmpty(criteria.getOrderName() ) ){
			srb.addSort(criteria.getOrderName()  , criteria.getSortOrder()) ; //设置排序 
		}
		if(criteria.getCurPage()!=null && criteria.getStartRow()!=null ){
			srb.setFrom(criteria.getStartRow() ).setSize(criteria.getPageSize()  ) ; //设置排序 
		}	
		SearchResponse response =srb.execute().actionGet();
	 
		return response.getHits().getTotalHits()  ; 
	}
	
	/**
	 * 查询条件为 组合 为   或者 
	 * @param criteria 索引  排序   分页 的条件 
	 * @param listQuery 查询的条件 列表 
	 * @return
	 */
	private  List<Map<String,Object>> executeOrQuery(ESQueryCriteria criteria , List<QueryBuilder> listQuery ){
		 
		SearchRequestBuilder srb =  client.prepareSearch().setIndices( criteria.getIndex())
				.setSearchType(SearchType.QUERY_THEN_FETCH  ); 
		if(StringUtils.isNotEmpty( criteria.getType())){
			srb.setTypes( criteria.getType() ) ; 
		}
		if( listQuery!=null && listQuery.size()>0){
			//Or 查询 
			for( QueryBuilder queryBuilder : listQuery){
				srb.setQuery(queryBuilder   ) ; 
			}
		}else{
			srb.setQuery( QueryBuilders.matchAllQuery() ) ; 
		}
		
		if(StringUtils.isNotEmpty(criteria.getOrderName() ) ){
			srb.addSort(criteria.getOrderName()  , criteria.getSortOrder()) ; //设置排序 
		}
		if(criteria.getCurPage()!=null && criteria.getStartRow()!=null ){
			srb.setFrom(criteria.getStartRow() ).setSize(criteria.getPageSize()  ) ; //设置排序 
		}	
		SearchResponse response =srb.execute().actionGet();
		SearchHits hits = response.getHits();
		 
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (int i = 0; i < hits.getHits().length; i++) {
			Map<String,Object> map = JsonUtils.strToMap(hits.getHits()[i].getSourceAsString() );
			map.put("es_id", hits.getHits()[i].getId()  ) ;  
			map.put("es_index", hits.getHits()[i].getIndex()  ) ;
			map.put("es_type", hits.getHits()[i].getType() ) ; 
			list.add( map); 
			//System.out.println(hits.getHits()[i].getSourceAsString());
			logger.debug( hits.getHits()[i].getSourceAsString());
		}
		return list ; 
	}
	
	/**
	 * 查询所有  And 
	 * @param criteria
	 * @param listQuery
	 * @return
	 */
	public  List<Map<String,Object>> queryAndCriteria(ESQueryCriteria criteria,List<QueryBuilder> listQuery){
		return executeAndQuery(criteria , listQuery );
	}
	
	/**
	 * 查询所有 Or ,查询字段中每个字段只查询一次
	 * @param criteria
	 * @param listQuery
	 * @return
	 */
	public  List<Map<String,Object>> queryOrCriteria(ESQueryCriteria criteria,List<QueryBuilder> listQuery){
		return executeOrQuery(criteria , listQuery );
	}
	
	/**
	 * 查询数量
	 * @param criteria
	 * @param listQuery
	 * @return
	 */
	public  Long getCount(ESQueryCriteria criteria,List<QueryBuilder> listQuery){
		return executeCount(criteria , listQuery );
	}	
	
	/**
	 * @param index  索引
	 * @param field  那个字段
	 * @param text   搜索的文本
	 * @param size   返回多少数据
	 * @return
	 */
	public   List<String> getCompletionSuggest(String index,String field,String text, int size ) {
		CompletionSuggestionBuilder suggestionsBuilder = new CompletionSuggestionBuilder("name_complete");
		suggestionsBuilder.text(text);
		suggestionsBuilder.field(field);
		suggestionsBuilder.size(size);
		SuggestResponse resp = client.prepareSuggest(index)
				.addSuggestion(suggestionsBuilder)
				.execute().actionGet();
//		System.out.println( resp.getSuggest() );
		List<? extends Entry<? extends Option>> list = resp.getSuggest().getSuggestion("name_complete").getEntries();
		List<String> suggests = new ArrayList<String>();
		if (list == null) {
			return null;
		} else {
			for (Entry<? extends Option> e : list) {
				for (Option option : e) {
					suggests.add(option.getText().toString());
				}
			}
			return suggests;
		}
	}
}