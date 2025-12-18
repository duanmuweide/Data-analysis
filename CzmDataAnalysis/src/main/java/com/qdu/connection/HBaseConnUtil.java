package com.qdu.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase连接工具类（当前业务未使用，但预留接口）
 */
public class HBaseConnUtil {
  // HBase配置
  private static final String HBASE_ZK_QUORUM = "master-pc"; // Zookeeper地址
  private static final String HBASE_ZK_PORT = "2181"; // Zookeeper端口

  private static Configuration conf = null;
  private static Connection conn = null;

  static {
    // 初始化HBase配置
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT);
  }

  /**
   * 获取HBase连接
   * @return Connection
   * @throws IOException 连接异常
   */
  public static Connection getHBaseConnection() throws IOException {
    if (conn == null || conn.isClosed()) {
      conn = ConnectionFactory.createConnection(conf);
    }
    return conn;
  }

  /**
   * 关闭HBase连接
   */
  public static void closeConnection() {
    if (conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        System.err.println("关闭HBase连接失败：" + e.getMessage());
      }
    }
  }
}