package com.qdu.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Hive JDBC连接工具类
 */
public class HiveConnUtil {
  // Hive连接配置
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  static {
    // 静态代码块加载Hive驱动
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      System.err.println("Hive驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 获取Hive连接
   * @return Connection
   * @throws SQLException 连接异常
   */
  public static Connection getHiveConnection() throws SQLException {
    Properties hiveProps = new Properties();
    hiveProps.setProperty("user", HIVE_USER);
    hiveProps.setProperty("password", HIVE_PASSWORD);
    return DriverManager.getConnection(HIVE_URL, hiveProps);
  }

  /**
   * 关闭Hive连接
   * @param conn 连接对象
   */
  public static void closeConnection(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        System.err.println("关闭Hive连接失败：" + e.getMessage());
      }
    }
  }

  /**
   * 关闭Hive连接及Statement
   * @param conn 连接对象
   * @param stmt Statement对象
   */
  public static void closeConnection(Connection conn, java.sql.Statement stmt) {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        System.err.println("关闭Hive Statement失败：" + e.getMessage());
      }
    }
    closeConnection(conn);
  }
}