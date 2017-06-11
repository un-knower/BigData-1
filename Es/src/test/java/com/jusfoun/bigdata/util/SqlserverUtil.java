package com.jusfoun.bigdata.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.jusfoun.bigdata.util.es.ESUtil;
import com.jusfoun.bigdata.util.es.IndexEntity;

public class SqlserverUtil {
	 
	 public static String sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	 public static String sDBUrl1 = "jdbc:sqlserver://192.168.4.210;databaseName="; 
	 public static ESUtil esUtil = new ESUtil("es_prod23" , "192.168.4.207",9300 ); 
	 
	 public static Logger logger = Logger.getLogger("fileoutsucess"); 
	 public static Logger logger2 = Logger.getLogger("fileoutnodata"); 
	 public static Logger logger_error = Logger.getLogger("fileouterror"); 
	 
	 public static String  index_ = "enterprise_index" ; 
	  
	 public static void main(String[] args) throws SQLException {
		 SqlserverUtil s = new SqlserverUtil();
		 try {
			Class.forName(sDriverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}  
		 s.father();
	}
	 public  void father(){
		 Connection _CONN = null; 
		 try {             
	            String sDBUrl =sDBUrl1+ "EntDataCenter";  
	           
	            _CONN = DriverManager.getConnection(sDBUrl, "jusfoun", "9cf@123.com");  
	            
	    		
	         int curpage=1;
	         String sql1 = getzhuNextPageSql(curpage);
	   		// boolean flag = true;
	         
	   		 while(true){
	   			 
	   			
	   			 PreparedStatement ps = _CONN.prepareStatement(sql1);  
	   			 ResultSet rs = ps.executeQuery();  
	   			long count = 0;
   				 while(rs.next()){
   					 Map<String , String> map = new HashMap<String, String>();
   					 map.put("patID", rs.getString(1));
   					 map.put("entName", rs.getString("entName"));
   					 map.put("lawPersion", rs.getString("lawPersion"));
   					 map.put("lawPersionSpec", rs.getString("lawPersionSpec"));
   					 map.put("entAddress", rs.getString("entAddress"));
   					 map.put("business", rs.getString("business"));
   					 map.put("职工人数Num", rs.getString("职工人数Num"));
   					 map.put("employeesCount", rs.getString("employeesCount"));
   					 map.put("TSG", rs.getString("TSG"));
   					 map.put("province", rs.getString("province"));
   					 map.put("provinceCn", rs.getString("provinceCn"));
   					 map.put("city", rs.getString("city"));
   					 map.put("cityCn", rs.getString("cityCn"));
   					 map.put("district", rs.getString("district"));
   					 map.put("districtCn", rs.getString("districtCn"));
   					 map.put("GCID", rs.getString("GCID"));
   					 map.put("GCN", rs.getString("GCN"));
   					 map.put("hasMa", rs.getString("hasMa"));
   					 map.put("hasIpo", rs.getString("hasIpo"));
   					 map.put("hasRaise", rs.getString("hasRaise"));
   					 map.put("financeCount", rs.getString("financeCount"));
   					 map.put("mapLat", rs.getString("mapLat"));
   					 map.put("mapLng", rs.getString("mapLat"));
   					 map.put("lonLat", rs.getString("lonLat"));
   					 map.put("assetsGrowthRate", rs.getString("assetsGrowthRate"));
   					 map.put("revenueGrowthRate", rs.getString("assetsGrowthRate"));
   					 map.put("assetsAmounted", rs.getString("assetsGrowthRate"));
   					 map.put("annualRevenueRMB", rs.getString("assetsGrowthRate"));
   					 map.put("entShortName", rs.getString("entShortName"));
   					 map.put("regTime", rs.getString("regTime"));
   					 map.put("currency", rs.getString("currency"));
   					 map.put("IDMS", rs.getString("IDMS"));
   					 map.put("FPS", rs.getString("FPS"));
   					 map.put("PSP", rs.getString("PSP"));
   					 map.put("financialDemand", rs.getString("financialDemand"));
   					 map.put("annualRevenue", rs.getString("annualRevenue"));
   					 map.put("totalassets", rs.getString("totalassets"));
   					 map.put("operatingStatus", rs.getString("operatingStatus"));
   					 map.put("registeredcapital", rs.getString("registeredcapital"));
   					 map.put("registrationNumber", rs.getString("registrationNumber"));
   					 map.put("organizationCode", rs.getString("organizationCode"));
   					 map.put("registerAddress", rs.getString("registerAddress"));
   					String id =  esUtil.createIndex(index_ , "enterpriseinfo", map);
   					child(id,map.get("entName"));
   					logger.error( "主表 = 当前页数=" +curpage+" 企业名称： "+rs.getString("entName"));
   					count++ ; 
   				 }
   				 if(count<=0){
   					 //flag=false ;
   					 break ; 
   				 }
	   			 
	   			 curpage++;
	   			 sql1 = getzhuNextPageSql(curpage);
	   			 
	   			
	   		 }
	   		 
	   		 esUtil.close(); 
	   		 closeConn(_CONN);
	        } catch (Exception ex) {  
	           // ex.printStackTrace();  
	             System.out.println(ex.getMessage());  
	            logger.error(ex.getMessage()  );
	        }  
	 }
	 
	 /**
	  * 
	  * @param ci 
	  */
	 public String getzhuNextPageSql( Integer curpage){
		 
 		 int i=curpage;
   		 int pageSize = 1000;
   		 int pageNo = pageSize*(i-1);
   	 
   		 String sql1 = "SELECT TOP "+pageSize+" "+
   						"a.ent_id AS entId,"+
   						"ISNULL(a.企业名称, '') AS entName,"+
   						"ISNULL(a.法人代表_姓名, '') AS lawPersion,"+
   						"ISNULL(a.法人代表_姓名, '') AS lawPersionSpec,"+
   						"ISNULL(a.企业地址, '') AS entAddress,"+
   						"ISNULL(a.主要业务活动1, '') AS business,"+
   						"ISNULL(a.职工人数Num, 0) AS 职工人数Num,"+
   						"ISNULL(a.从业人数, 0) AS employeesCount,"+
   						"ISNULL(a.产品及服务, '') AS TSG,"+
   						"ISNULL(a.Province, 0) AS province,"+
   						"ISNULL(a.所在省份, '') AS provinceCn,"+
   						"ISNULL(a.City, 0) AS city,"+
   						"ISNULL(a.所在城市, '') AS cityCn,"+
   						"ISNULL(a.City, 0) AS district,"+
   						"ISNULL(a.所在区域, '') AS districtCn,"+
   						"ISNULL(a.国代分类代码, '') AS GCID,"+
   						"ISNULL(a.国代分类名称, '') AS GCN,"+
   						"ISNULL(a.has_ma, 0) AS hasMa,"+
   						"ISNULL(a.has_ipo, 0) AS hasIpo,"+
   						"ISNULL(a.has_raise, 0) AS hasRaise,"+
   						"ISNULL(a.finance_count, 0) AS financeCount,"+
   						"ISNULL(a.map_lat, 0) AS mapLat,"+
   						"ISNULL(a.map_lng, 0) AS mapLng,"+
   						"CONCAT ("+
   						"	CONVERT ("+
   						"		VARCHAR (20),"+
   						"		ISNULL(a.map_lng, 0)"+
   						"	),"+
   						"	' ',"+
   						"	CONVERT ("+
   						"		VARCHAR (20),"+
   						"		ISNULL(a.map_lat, 0)"+
   						"	)"+
   						") AS lonLat,"+
   					"	ISNULL(a.assets_growth_rate, 0) AS assetsGrowthRate,"+
   					"	ISNULL(a.revenue_growth_rate, 0) AS revenueGrowthRate,"+
   					"	ISNULL(a.资产总计_人民币, 0) AS assetsAmounted,"+
   					"	ISNULL("+
   					"		a.全年营业收入_人民币,"+
   					"		0"+
   					"	) AS annualRevenueRMB,"+
   					"	ISNULL(a.企业简称, '') AS entShortName,"+
   					"	ISNULL("+
   					"		a.企业成立时间,"+
   					"		'1980-01-01'"+
   					"	) AS regTime,"+
   					"	'' AS TSG2,"+
   					"	ISNULL(a.币种, 0) AS currency,"+
   					"	ISNULL(a.国内上市板块, '') AS IDMS,"+
   					"	ISNULL(a.国外上市板块, '') AS FPS,"+
   					"	ISNULL(a.上市板块阶段, '') AS PSP,"+
   					"	ISNULL(a.FinancingDemand, '') AS financialDemand,"+
   					"	ISNULL(a.全年营业收入, 0) AS annualRevenue,"+
   					"	ISNULL(a.资产总计, 0) AS totalassets,"+
   					"	ISNULL(a.企业营业状态, '') AS operatingStatus,"+
   					"	ISNULL(a.注册资金, 0) AS registeredcapital,"+
   					"	ISNULL("+
   					"		a.登记注册号_工商,"+
   					"		''"+
   					"	) AS registrationNumber,"+
   					"	ISNULL(a.组织机构代码, 0) AS organizationCode,"+
   					"	ISNULL(a.单位注册地址, 0) AS registerAddress "+
   					" FROM"+
   					" ent_all AS a "+
   					" WHERE     a.ent_id >"+
   					 "         ("+
   					  "        SELECT ISNULL(MAX(a.ent_id),0) "+
   					"          FROM "+
   					"                ("+
   					"                SELECT TOP "+pageNo+" b.ent_id FROM ent_all b  ORDER BY b.ent_id"+
   					"                ) A"+
   					"          )"+
   					"ORDER BY a.ent_id";
   		 return sql1 ;  
	 }
	 
//	 where b.企业名称 = 'TCL集团股份有限公司'	 a.企业名称='TCL集团股份有限公司' and
	 public void child(String id,String name){
		Connection _CONN = null; 
		UUID uuid = UUID.randomUUID();
		IndexEntity indexEntity = new IndexEntity(); 
		indexEntity.setIndex(index_);
		indexEntity.setId( uuid.toString()  );
		indexEntity.setType( "patent_child");
		indexEntity.setParentId(id);
		
		 try {             
	            String sDBUrl = sDBUrl1+"EntDataPatent";   
	            _CONN = DriverManager.getConnection(sDBUrl, "jusfoun", "9cf@123.com");  
 
	   		 String sql = "SELECT"+
	   					" ID AS patID,"+
	   					" ("+
	   					"	CASE patType"+
	   					"	WHEN 1 THEN"+
	   					"		'发明专利'"+
	   					"	WHEN 2 THEN"+
	   					"		'实用新型'"+
	   					"	WHEN 3 THEN"+
	   					"		'外观专利'"+
	   					"	WHEN 8 THEN"+
	   					"		'PCT发明'"+
	   					"	WHEN 9 THEN"+
	   						"	'PCT实用新型'"+
	   						" END"+
	   					" ) patType,"+
	   					" appDate AS patTime,"+
	   					" 0 AS patYear,"+
	   					" applicantName "+
	   				" FROM "+
	   				"	Ent_PatentInfo_a where applicantName ='"+name+"'";
	   		 //boolean flag = true;
   			 PreparedStatement ps = _CONN.prepareStatement(sql);  
   			 ResultSet rs = ps.executeQuery(); 
   			 Long count =0l ; 
			 while(rs.next()){
				 Map<String , String> map = new HashMap<String, String>();
				 map.put("patID", rs.getString("patID"));
				 map.put("patType", rs.getString("patType"));
				 map.put("patTime", rs.getString("patTime"));
				 map.put("patYear", rs.getString("patYear"));
				 map.put("applicantName", rs.getString("applicantName"));
				 String cid =  esUtil.insertData( indexEntity, map);
				 logger2.error( "子表 = 当前id     patID="+rs.getString(1));
				 count++ ; 
			 }
	   		
	   		 closeConn(_CONN);
	        } catch (Exception ex) {  
//	            ex.printStackTrace();  
	            logger_error.error(ex.getMessage() );
	            System.out.println(ex.getMessage());  
	        }  
		 
	 }
	    //关闭连接  
	    private void closeConn(Connection _CONN)  
	    {  
	        try {  
	        	if(_CONN!=null){
		            _CONN.close();  
		            _CONN = null;  	
	        	}
	        } catch (Exception ex) {  
	            System.out.println(ex.getMessage());  
	            logger_error.error(ex.getMessage() );
	            _CONN=null;   
	        }  
	    }  
	   
}
