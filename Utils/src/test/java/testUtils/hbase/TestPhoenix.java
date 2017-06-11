package testUtils.hbase;

import org.junit.Test;
import somethingUtils.hbase.HBaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestPhoenix {

 
	  @Test
	  public  void test2() { 
		  HBaseHelper base = new HBaseHelper();
	      try {  
	          String tableName = "test";  
	          // 第一步：创建数据库表：“student”  
	          String[] columnFamilys = { "info", "course" };  
//	          base.create(tableName, columnFamilys);  
	          // 第二步：向数据表的添加数据  
	          // 添加第一行数据  
	        
//      	  
//      	  base.put(tableName, "zpc", "info", "age", "20");  
//            base.put(tableName, "zpc", "info", "sex", "boy");  
//            base.put(tableName, "zpc", "course", "china", "97");  
//            base.put(tableName, "zpc", "course", "math", "128");  
//            base.put(tableName, "zpc", "course", "english", "85");  
//            // 添加第二行数据  
//            base.put(tableName, "henjun", "info", "age", "19");  
//            base.put(tableName, "henjun", "info", "sex", "boy");  
//            base.put(tableName, "henjun", "course", "china","90");  
//            base.put(tableName, "henjun", "course", "math","120");  
//            base.put(tableName, "henjun", "course", "english","90");  
//            // 添加第三行数据  
//            base.put(tableName, "niaopeng", "info", "age", "18");  
//            base.put(tableName, "niaopeng", "info", "sex","girl");  
//            base.put(tableName, "niaopeng", "course", "china","100");  
//            base.put(tableName, "niaopeng", "course", "math","100");  
//            base.put(tableName, "niaopeng", "course", "english","99");  
            // 第三步：获取一条数据  
//            System.out.println("**************获取一条(zpc)数据*************");  
//            base.get(tableName, "zpc");  
            // 第四步：获取所有数据  
            System.out.println("**************获取所有数据***************");  
            List<String> list = base.getAllColumnFamily(tableName);  
            System.out.println( list );
            // 第五步：删除一条数据  
            System.out.println("************删除一条(zpc)数据************");  
            List<String> list2 =  base.getRowKeyList(tableName,"1","60" );
            System.out.println( list2 );
            
            
            List<String> rowKeys = new ArrayList<String>() ;
            rowKeys.add("zpc" ) ; 
            List<Map<String, Map<String, String>>> list3 =  base.get(tableName, rowKeys) ; 
            System.out.println( list3 );
            
//            base.delRow(tableName, "zpc");  
//            base.getAllRows(tableName);  
//            // 第六步：删除多条数据  
//            System.out.println("**************删除多条数据***************");  
//            String rows[] = new String[] { "qingqing","xiaoxue" };  
//            base.delMultiRows(tableName, rows);  
//            base.getAllRows(tableName);  
//            // 第七步：删除数据库  
//            System.out.println("***************删除数据库表**************");  
//            base.deleteTable(tableName);  
//            System.out.println("表"+tableName+"存在吗？"+isExist(tableName));  
	       
	  
	      } catch (Exception e) {  
	          e.printStackTrace();  
	      }  
	  }  
	  
	  
	  
	  @Test
	  public   void test1() { 
		  HBaseHelper base = new HBaseHelper(); 
	      try {  
	          String tableName = "student";  
	          // 第一步：创建数据库表：“student”  
	          String[] columnFamilys = { "info", "course" };  
	          base.create(tableName, columnFamilys);  
	          // 第二步：向数据表的添加数据  
	          // 添加第一行数据  
	        
        	  
        	  base.put(tableName, "zpc", "info", "age", "20");  
              base.put(tableName, "zpc", "info", "sex", "boy");  
              base.put(tableName, "zpc", "course", "china", "97");  
              base.put(tableName, "zpc", "course", "math", "128");  
              base.put(tableName, "zpc", "course", "english", "85");  
              // 添加第二行数据  
              base.put(tableName, "henjun", "info", "age", "19");  
              base.put(tableName, "henjun", "info", "sex", "boy");  
              base.put(tableName, "henjun", "course", "china","90");  
              base.put(tableName, "henjun", "course", "math","120");  
              base.put(tableName, "henjun", "course", "english","90");  
              // 添加第三行数据  
              base.put(tableName, "niaopeng", "info", "age", "18");  
              base.put(tableName, "niaopeng", "info", "sex","girl");  
              base.put(tableName, "niaopeng", "course", "china","100");  
              base.put(tableName, "niaopeng", "course", "math","100");  
              base.put(tableName, "niaopeng", "course", "english","99");  
              // 第三步：获取一条数据  
//              System.out.println("**************获取一条(zpc)数据*************");  
//              base.get(tableName, "zpc");  
              // 第四步：获取所有数据  
              System.out.println("**************获取所有数据***************");  
              List<String> list = base.getAllColumnFamily(tableName);  
              System.out.println( list );
              // 第五步：删除一条数据  
              System.out.println("************删除一条(zpc)数据************");  
              List<String> list2 =  base.getRowKeyList(tableName,"1","60" );
              System.out.println( list2 );
              
              
              List<String> rowKeys = new ArrayList<String>() ;
              rowKeys.add("zpc" ) ; 
              List<Map<String, Map<String, String>>> list3 =  base.get(tableName, rowKeys) ; 
              System.out.println( list3 );
              
//              base.delRow(tableName, "zpc");  
//              base.getAllRows(tableName);  
//              // 第六步：删除多条数据  
//              System.out.println("**************删除多条数据***************");  
//              String rows[] = new String[] { "qingqing","xiaoxue" };  
//              base.delMultiRows(tableName, rows);  
//              base.getAllRows(tableName);  
//              // 第七步：删除数据库  
//              System.out.println("***************删除数据库表**************");  
//              base.deleteTable(tableName);  
//              System.out.println("表"+tableName+"存在吗？"+isExist(tableName));  
	       
	  
	      } catch (Exception e) {  
	          e.printStackTrace();  
	      }  
	  }  
	  
}
