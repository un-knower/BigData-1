package somethingUtils.phoenix;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhoenixDao {

	/**
	 * create table test7 (mykey integer not null primary key, mycolumn varchar)
	 * upsert into test7 values (1,'Hello') 
	 * @param sql
	 * @return
	 */
	public static int execute(String sql ){
		Connection conn = PhoenixUtil.getConnection() ;
		Statement  stmt = null ;
		int rows=0 ; 
		try {
			stmt = conn.createStatement();
			rows = stmt.executeUpdate( sql );
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		PhoenixUtil.closeStml(stmt);
		PhoenixUtil.closeConn();
		return rows ;
	}
	
	/**
	 * 查询功能
	 * @param sql
	 * @return 
	 * @throws SQLException
	 */
	public static List<Map<String,Object>> query(String sql )  {
		
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		Connection conn = PhoenixUtil.getConnection() ;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql );
			ResultSet rset = stmt.executeQuery();
			
			List<String> listColumn = getColumnName( rset );
			
			while (rset.next()) {
				Map<String,Object> map = new HashMap<String,Object>();
				for(int i=0;i<listColumn.size();i++ ){
					String name = listColumn.get(i); 
					Object value = rset.getObject( name);
					map.put( name,value ) ; 
				}
				list.add(map) ; 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("查询出来的数据："+ list ); 
		
		PhoenixUtil.closeStml(stmt);
		PhoenixUtil.closeConn();
		return list ;
	}
	
	/**
	 * 获取列名
	 * @param rset
	 * @return
	 */
	private static List<String> getColumnName( ResultSet rset ){
		List<String> columnList = new ArrayList<String>();
		
		try {
			ResultSetMetaData rsd = rset.getMetaData();
			int  count = rsd.getColumnCount() ; 
			for(int i=1;i<=count;i++ ){
				String name = rsd.getColumnName(i);
				columnList.add(name);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return columnList ; 
		
	}
}
