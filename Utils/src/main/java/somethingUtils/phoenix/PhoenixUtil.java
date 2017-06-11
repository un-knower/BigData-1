package somethingUtils.phoenix;

import java.sql.*;

/**
 * 数据库连接工具类
 * 
 * @author wzt
 */
public class PhoenixUtil {

	private static final ThreadLocal<ConnManger> threadLocal = new ThreadLocal<ConnManger>();

	/**
	 * 事务管理辅助类
	 */
	private static class ConnManger{
		private Connection conn = null;

		public void close() {
			if (conn != null) {
				try {
					conn.close();
					conn = null;
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	/**
	 * 如果threadLocal中已经存在ConnManger（如调用过begin方法），这返回ConnManger中的Connection，
	 * 否则新建一个ConnManger，然后创建一个Connection放到ConnManger中，
	 * 然后把ConnManger放到threadLocal中，并返回Connection
	 * 
	 * @return 连接好数据库的java.sql.Connection
	 */
	public static Connection getConnection() {
		ConnManger connManger = threadLocal.get();
		if (connManger == null) {
			connManger = new ConnManger();
			try {
				connManger.conn = DriverManager.getConnection( "jdbc:phoenix:ubt202,ubt204,ubt203:2181:/hbase");
				threadLocal.set(connManger);
			} catch (SQLException e) {
				connManger.close();
				threadLocal.remove();
				e.printStackTrace();
			}
		}
		return connManger.conn;
	}
 

	/**
	 * 关闭java.sql.Connection
	 */
	public static void closeConn() {
		ConnManger connManger = threadLocal.get();
		if (connManger!=null && connManger.conn != null) {
			connManger.close();
			threadLocal.remove();
		}
	}

	/**
	 * 关闭java.sql.Statement
	 * 
	 * @param stml
	 *            需要关闭的java.sql.Statement
	 */
	public static void closeStml(Statement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * 关闭java.sql.ResultSet
	 * 
	 * @param rs
	 *            关闭java.sql.ResultSet
	 */
	public static void closeRs(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
 
}