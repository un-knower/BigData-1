package com.hsw.presto;

import java.sql.*;
/**
 * Created by hushiwei on 2017/10/12.
 * desc :
 */
public class PrestoJdbcDemo {

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    Connection connection = DriverManager
        .getConnection("jdbc:presto://10.10.25.14:8585/hive/default","root",null);
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("show tables");
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }
    rs.close();
    connection.close();

  }
}
