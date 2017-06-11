package somethingUtils.mysql;

import somethingUtils.xml.ConfigFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据库连接工具类
 * 
 * @author wzt
 */
public class MysqlDatabaseUtil {

	private static final ThreadLocal<AutoCommit> threadLocal = new ThreadLocal<AutoCommit>();
	static {
		try {
			Class.forName(ConfigFactory.getString("mysql.driverClass"));
		} catch (ClassNotFoundException ex) {
			System.out.println("数据库驱动加载失败");
		}
	}

	/**
	 * 事务管理辅助类
	 */
	private static class AutoCommit {
		private Connection conn = null;
		private boolean autoCommit = true;

		public void close() {
			if (conn != null) {
				try {
					conn.close();
					conn = null;
					autoCommit = true;
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	/**
	 * 如果threadLocal中已经存在AutoCommit（如调用过begin方法），这返回AutoCommit中的Connection，
	 * 否则新建一个AutoCommit，然后创建一个Connection放到AutoCommit中，
	 * 然后把AutoCommit放到threadLocal中，并返回Connection
	 * 
	 * @return 连接好数据库的java.sql.Connection
	 */
	public static Connection getConnection() {
		AutoCommit autoCommit = threadLocal.get();
		if (autoCommit == null) {
			autoCommit = new AutoCommit();
			try {
				//jdbc:mysql://192.168.4.213:3306/jusfoun_bigdata?characterEncoding=utf8
				autoCommit.conn = DriverManager.getConnection(ConfigFactory.getString("mysql.url"), 
						ConfigFactory.getString("mysql.username"),
						ConfigFactory.getString("mysql.password"));
				threadLocal.set(autoCommit);
			} catch (SQLException e) {
				autoCommit.close();
				threadLocal.remove();
				e.printStackTrace();
			}
		}
		return autoCommit.conn;
	}
 

	/**
	 * 如果threadLocal中的AutoCommit不为null，
	 * 这创建一个AutoCommit并且创建数据库连接Connection并打开事务管理 然后将AutoCommit放到threadLocal中
	 */
	public static void begin( ) {
		AutoCommit autoCommit = threadLocal.get();
		if (autoCommit == null) {
			autoCommit = new AutoCommit();
			threadLocal.set(autoCommit);
			try {
				autoCommit.conn = DriverManager.getConnection(ConfigFactory.getString("mysql.url"), 
						ConfigFactory.getString("mysql.username"),
						ConfigFactory.getString("mysql.password"));
				autoCommit.conn.setAutoCommit(false);
				autoCommit.autoCommit = false;
			} catch (SQLException e) {
				autoCommit.close();
				threadLocal.remove();
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭java.sql.Connection
	 */
	public static void closeConn() {
		AutoCommit autoCommit = threadLocal.get();
		if (autoCommit.autoCommit && autoCommit.conn != null) {
			autoCommit.close();
			threadLocal.remove();
		}
	}

	/**
	 * 关闭java.sql.Statement
	 * 
	 * @param stml
	 *            需要关闭的java.sql.Statement
	 */
	public static void closeStml(Statement stml) {
		if (stml != null) {
			try {
				stml.close();
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

	/**
	 * 当事务管理启动后，调用此方法进行提交事物，并且关闭java.sql.Connection
	 */
	public static void commit() {
		AutoCommit autoCommit = threadLocal.get();
		if (!autoCommit.autoCommit && autoCommit.conn != null) {
			try {
				autoCommit.conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				autoCommit.close();
				threadLocal.remove();
			}
		}
	}

	/**
	 * 当事务处理失败时调用此方法进行事务回滚，并且关闭java.sql.Connection
	 */
	public static void rollback() {
		AutoCommit autoCommit = threadLocal.get();
		if (!autoCommit.autoCommit && autoCommit.conn != null) {
			try {
				autoCommit.conn.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				autoCommit.close();
				threadLocal.remove();
			}
		}
	}
}