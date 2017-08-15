package somethingUtils.mysql;

import java.sql.*;

/**
 * Created by hushiwei on 2017/8/4.
 */
public class readmysql {

    private static String dmpJdbcUrl = "jdbc:mysql://10.10.25.11:3306/dmp?useUnicode=true&characterEncoding=utf-8";
    private static String dmpUser = "root";
    private static String dmpPasswd = "Oj8nSfQZc4BsS2xv";


    public static void main(String[] args) {
        Connection conn = null;
        Statement  stmt = null;


        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dmpJdbcUrl, dmpUser, dmpPasswd);// 获取连接

            stmt = conn.createStatement();
            String    sql       = "select info_combination from dmp_ads_data";
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String string = resultSet.getString(1);
                System.out.println(string);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
