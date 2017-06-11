package com.jusfoun.bigdata.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jusfoun.bigdata.util.es.BulkProcessBuilder;
import com.jusfoun.bigdata.util.es.ESUtilAdvance;

public class Test2 implements Runnable {

	public static void main(String[] args) {
		
		ESUtilAdvance esUtil = new ESUtilAdvance("es-ncp-cluster1" , "192.168.4.203,192.168.4.205,192.168.4.206",9300 );
		
		esUtil.createIndex("test2");
		esUtil.close();
		//new Thread(new Test2() ).start();
	}

	@Override
	public void run() {
		ESUtilAdvance esUtil = new ESUtilAdvance("es-ncp-cluster1" , "192.168.4.203,192.168.4.205,192.168.4.206",9300 ); 
		List<Map<String,Object>> listMap = new ArrayList<Map<String,Object>>();
		
		Map map = new HashMap();
		map.put("COUNTRYCODE", "4" ) ; 
		map.put("ID", "45") ; 
		map.put("COUNTRYNAME",  "COUNTRYNAME1") ;
		map.put("CREATETIME",  "2016-03-29 10:09:23") ;

		
		//esUtil.insertData("jusfoun","COUNTRY_TEST2",  map) ; 
		//esUtil.insertData("jusfoun","COUNTRY_TEST2",map) ;
		//esUtil.bulkProcessorIndex("jusfoun","COUNTRY_TEST2", "ID", listMap);
		BulkProcessBuilder bulkProcessBuilder = new BulkProcessBuilder(esUtil);
//		listMap.add( map) ; 
		for(int i=0;i<100;i++ ){
			listMap.add( map) ; 	
			//bulkProcessBuilder.appendData("jusfoun","COUNTRY_TEST2", listMap, "ID");
		}
		bulkProcessBuilder.appendData("jusfoun","COUNTRY_TEST2", listMap, "ID");
		bulkProcessBuilder.closeBulk();
		
//		esUtil.bulkProcessorIndex("jusfoun","COUNTRY_TEST2",   "ID",listMap);
		esUtil.close();
	}
	
}
