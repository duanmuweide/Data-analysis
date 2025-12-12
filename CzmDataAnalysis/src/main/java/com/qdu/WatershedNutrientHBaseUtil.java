package com.qdu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 修复版：HBase查询 → 写入MySQL（解决Connection类型冲突、方法找不到问题）
 */
public class WatershedNutrientHBaseUtil {
  // -------------------------- 1. 核心配置（替换为你的实际信息） --------------------------
  // HBase配置
  private static final String HBASE_ZK = "master-pc";
  private static final String HBASE_TABLE = "watershed_hbase.nutrient_surplus";

  // MySQL配置（关键：替换为你的MySQL信息）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";       // 你的MySQL账号
  private static final String MYSQL_PWD = "Czm982376";// 你的MySQL密码
  private static final String MYSQL_TABLE = "watershed_surplus_trend"; // MySQL表名

  // HBase列定义
  private static final byte[] CF_BASIC = Bytes.toBytes("cf_basic");
  private static final byte[] CF_SURPLUS = Bytes.toBytes("cf_surplus");
  private static final byte[] COL_YEAR = Bytes.toBytes("year");
  private static final byte[] COL_N_SURPLUS = Bytes.toBytes("n_ag_surplus_kgsqkm");
  private static final byte[] COL_P_SURPLUS = Bytes.toBytes("p_ag_surplus_kgsqkm");

  // -------------------------- 2. 数据实体类 --------------------------
  static class TrendData {
    String fips;
    int year;
    double nSurplus;
    double pSurplus;

    TrendData(String fips, int year, double nSurplus, double pSurplus) {
      this.fips = fips;
      this.year = year;
      this.nSurplus = nSurplus;
      this.pSurplus = pSurplus;
    }
  }

  // -------------------------- 3. HBase配置初始化 --------------------------
  private static Configuration getHBaseConf() {
    Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", HBASE_ZK);
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    return conf;
  }

  // -------------------------- 4. HBase查询（显式用HBase的Connection） --------------------------
  private static List<TrendData> queryHBase(String fips, int startYear, int endYear) throws IOException {
    List<TrendData> trendList = new ArrayList<>();

    // 显式声明：HBase的Connection
    org.apache.hadoop.hbase.client.Connection hbaseConn = null;
    Table table = null;
    ResultScanner scanner = null;

    try {
      hbaseConn = ConnectionFactory.createConnection(getHBaseConf());
      table = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE));

      // 构建RowKey范围
      String revFips = new StringBuilder(fips).reverse().toString();
      String startRow = revFips + "_" + String.format("%04d", startYear);
      String stopRow = revFips + "_" + String.format("%04d", endYear + 1);

      // 扫描HBase数据
      Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
      scan.addColumn(CF_BASIC, COL_YEAR);
      scan.addColumn(CF_SURPLUS, COL_N_SURPLUS);
      scan.addColumn(CF_SURPLUS, COL_P_SURPLUS);

      scanner = table.getScanner(scan);
      for (Result res : scanner) {
        int year = Bytes.toInt(res.getValue(CF_BASIC, COL_YEAR));
        double n = res.containsColumn(CF_SURPLUS, COL_N_SURPLUS) ?
                Bytes.toDouble(res.getValue(CF_SURPLUS, COL_N_SURPLUS)) : 0.0;
        double p = res.containsColumn(CF_SURPLUS, COL_P_SURPLUS) ?
                Bytes.toDouble(res.getValue(CF_SURPLUS, COL_P_SURPLUS)) : 0.0;
        trendList.add(new TrendData(fips, year, n, p));
      }
      System.out.println("✅ HBase查询完成，共" + trendList.size() + "条数据");
    } finally {
      // 关闭HBase资源
      if (scanner != null) scanner.close();
      if (table != null) table.close();
      if (hbaseConn != null) hbaseConn.close();
    }
    return trendList;
  }

  // -------------------------- 5. 写入MySQL（显式用JDBC的Connection） --------------------------
  private static void writeToMySQL(List<TrendData> trendList) throws ClassNotFoundException, SQLException {
    if (trendList.isEmpty()) {
      System.out.println("⚠️ 无数据可写入MySQL");
      return;
    }

    // 加载MySQL驱动
    Class.forName("com.mysql.cj.jdbc.Driver");

    // 显式声明：JDBC的Connection（解决类型冲突核心！）
    java.sql.Connection mysqlConn = null;
    PreparedStatement pstmt = null;

    try {
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PWD);
      // 批量写入SQL（防重复）
      String sql = "INSERT INTO " + MYSQL_TABLE + " (fips, year, n_surplus, p_surplus) VALUES (?, ?, ?, ?) " +
              "ON DUPLICATE KEY UPDATE n_surplus=VALUES(n_surplus), p_surplus=VALUES(p_surplus)";
      pstmt = mysqlConn.prepareStatement(sql);

      // 关闭自动提交，批量执行
      mysqlConn.setAutoCommit(false);
      int count = 0;

      for (TrendData data : trendList) {
        pstmt.setString(1, data.fips);
        pstmt.setInt(2, data.year);
        pstmt.setDouble(3, data.nSurplus);
        pstmt.setDouble(4, data.pSurplus);
        pstmt.addBatch();
        count++;

        // 每100条提交一次
        if (count % 100 == 0) {
          pstmt.executeBatch();
          mysqlConn.commit();
          System.out.println("✅ MySQL已写入" + count + "条数据");
        }
      }

      // 提交剩余数据
      pstmt.executeBatch();
      mysqlConn.commit();
      System.out.println("✅ MySQL写入完成，累计" + count + "条数据");

    } catch (SQLException e) {
      // 回滚事务
      if (mysqlConn != null) mysqlConn.rollback();
      System.err.println("❌ MySQL写入失败：" + e.getMessage());
      throw e;
    } finally {
      // 关闭MySQL资源
      if (pstmt != null) pstmt.close();
      if (mysqlConn != null) mysqlConn.close();
    }
  }

  // -------------------------- 6. 主方法（全流程入口） --------------------------
  public static void main(String[] args) {
    try {
      // 1. HBase查询（替换为你的FIPS）
      List<TrendData> trendData = queryHBase("10005", 2010, 2020);

      // 2. 写入MySQL
      writeToMySQL(trendData);

      System.out.println("\n🎉 全流程执行完成：HBase查询 → MySQL写入");

    } catch (Exception e) {
      System.err.println("\n❌ 执行失败：" + e.getMessage());
      e.printStackTrace();
    }
  }
}