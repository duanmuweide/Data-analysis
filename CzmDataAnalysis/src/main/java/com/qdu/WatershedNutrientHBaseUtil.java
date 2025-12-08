package com.qdu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 流域营养盐盈余HBase操作工具类
 * 包含：建表、数据写入、趋势查询
 */
public class WatershedNutrientHBaseUtil {
  // HBase配置（需替换为集群实际地址）
  private static final String HBASE_ZK_QUORUM = "master-pc"; // ZK地址
  private static final String HBASE_ZK_PORT = "2181"; // ZK端口
  private static final String NAMESPACE = "watershed_hbase";
  private static final String TABLE_NAME = NAMESPACE + ".nutrient_surplus";

  // 列族定义
  public static final byte[] CF_BASIC = Bytes.toBytes("cf_basic");    // 基础信息
  public static final byte[] CF_SURPLUS = Bytes.toBytes("cf_surplus");// 盈余数据
  public static final byte[] CF_EMISSION = Bytes.toBytes("cf_emission");// 排放数据

  // 列名定义
  public static final byte[] COL_AREA_SQKM = Bytes.toBytes("area_sqkm");
  public static final byte[] COL_YEAR = Bytes.toBytes("year");
  public static final byte[] COL_N_AG_SURPLUS = Bytes.toBytes("n_ag_surplus_kgsqkm");
  public static final byte[] COL_P_AG_SURPLUS = Bytes.toBytes("p_ag_surplus_kgsqkm");
  public static final byte[] COL_N_EMIS_TOTAL = Bytes.toBytes("n_emis_total_kgsqkm");
  public static final byte[] COL_P_POINT_SOURCE = Bytes.toBytes("p_point_source_kgsqkm");

  /**
   * 初始化HBase配置
   */
  private static Configuration getHBaseConfig() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT);
    conf.set("hbase.client.retries.number", "3"); // 重试次数
    return conf;
  }

  /**
   * 1. 创建命名空间和表（仅需执行一次）
   */
  public static void createTable() throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
         Admin admin = conn.getAdmin()) {

      // Step1: 创建命名空间（如果不存在）
      NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(NAMESPACE)
              .addConfiguration("description", "流域营养盐数据存储")
              .build();
      try {
        admin.createNamespace(namespaceDesc);
        System.out.println("命名空间 " + NAMESPACE + " 创建成功");
      } catch (NamespaceExistException e) {
        System.out.println("命名空间 " + NAMESPACE + " 已存在，跳过创建");
      }

      // Step2: 创建表（多列族）
      TableName tableName = TableName.valueOf(TABLE_NAME);
      if (admin.tableExists(tableName)) {
        System.out.println("表 " + TABLE_NAME + " 已存在，跳过创建");
        return;
      }

      // 定义列族（设置版本数=1，仅保留最新值）
      ColumnFamilyDescriptor cfBasic = ColumnFamilyDescriptorBuilder.newBuilder(CF_BASIC)
              .setMaxVersions(1)
              .build(); // 去掉压缩配置，使用默认无压缩

      ColumnFamilyDescriptor cfSurplus = ColumnFamilyDescriptorBuilder.newBuilder(CF_SURPLUS)
              .setMaxVersions(1)
              .build();

      ColumnFamilyDescriptor cfEmission = ColumnFamilyDescriptorBuilder.newBuilder(CF_EMISSION)
              .setMaxVersions(1)
              .build();

      // 创建表
      TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
              .setColumnFamily(cfBasic)
              .setColumnFamily(cfSurplus)
              .setColumnFamily(cfEmission)
              .build();
      admin.createTable(tableDesc);
      System.out.println("表 " + TABLE_NAME + " 创建成功");
    }
  }

  /**
   * 2. RowKey生成工具（核心：反转FIPS + 补位年份）
   * @param fips 流域FIPS编码（如"38001"）
   * @param year 年份（如2020）
   * @return 格式：反转FIPS_补位年份（如"38001"反转→"10083"，拼接2020→"10083_2020"）
   */
  public static String generateRowKey(String fips, int year) {
    // Step1: 反转FIPS（避免连续FIPS导致热点）
    String reversedFips = new StringBuilder(fips).reverse().toString();
    // Step2: 年份补4位（避免"10001_9" < "10001_10"的排序问题）
    String paddedYear = String.format("%04d", year);
    // Step3: 拼接RowKey
    return reversedFips + "_" + paddedYear;
  }

  /**
   * 3. 写入单条流域数据到HBase
   * @param fips 流域FIPS
   * @param year 年份
   * @param areaSqkm 流域面积
   * @param nAgSurplus 氮农业盈余
   * @param pAgSurplus 磷农业盈余
   * @param nEmisTotal 总氮排放
   * @param pPointSource 点源磷排放
   */
  public static void putWatershedData(String fips, int year, double areaSqkm,
                                      double nAgSurplus, double pAgSurplus,
                                      double nEmisTotal, double pPointSource) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
         Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

      // 生成RowKey
      String rowKey = generateRowKey(fips, year);
      Put put = new Put(Bytes.toBytes(rowKey));

      // 写入基础信息列族
      put.addColumn(CF_BASIC, COL_YEAR, Bytes.toBytes(year));
      put.addColumn(CF_BASIC, COL_AREA_SQKM, Bytes.toBytes(areaSqkm));

      // 写入盈余列族
      put.addColumn(CF_SURPLUS, COL_N_AG_SURPLUS, Bytes.toBytes(nAgSurplus));
      put.addColumn(CF_SURPLUS, COL_P_AG_SURPLUS, Bytes.toBytes(pAgSurplus));

      // 写入排放列族
      put.addColumn(CF_EMISSION, COL_N_EMIS_TOTAL, Bytes.toBytes(nEmisTotal));
      put.addColumn(CF_EMISSION, COL_P_POINT_SOURCE, Bytes.toBytes(pPointSource));

      // 写入数据（批量写入可改用putBatch）
      table.put(put);
      System.out.println("数据写入成功：RowKey=" + rowKey);
    }
  }

  /**
   * 4. 核心场景：查询特定流域多年氮磷盈余变化趋势
   * @param targetFips 目标流域FIPS（原始值，如"38001"）
   * @param startYear 起始年份
   * @param endYear 结束年份
   */
  public static void querySurplusTrend(String targetFips, int startYear, int endYear) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
         Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

      // Step1: 构建扫描范围（基于反转FIPS）
      String reversedFips = new StringBuilder(targetFips).reverse().toString();
      // 起始RowKey：反转FIPS_起始年份（补4位）
      String startRowKey = reversedFips + "_" + String.format("%04d", startYear);
      // 结束RowKey：反转FIPS_结束年份+1（HBase stopRow是开区间）
      String stopRowKey = reversedFips + "_" + String.format("%04d", endYear + 1);

      // Step2: 构建扫描器（只查需要的列，减少IO）
      Scan scan = new Scan();
      scan.withStartRow(Bytes.toBytes(startRowKey));
      scan.withStopRow(Bytes.toBytes(stopRowKey));
      // 指定列族+列，避免全表扫描
      scan.addColumn(CF_BASIC, COL_YEAR);
      scan.addColumn(CF_SURPLUS, COL_N_AG_SURPLUS);
      scan.addColumn(CF_SURPLUS, COL_P_AG_SURPLUS);
      // 设置缓存（提升扫描效率）
      scan.setCaching(100);
      scan.setCacheBlocks(false);

      // Step3: 执行扫描并处理结果
      ResultScanner scanner = table.getScanner(scan);
      System.out.printf("===== 流域%s（%d-%d年）氮磷盈余趋势 =====%n", targetFips, startYear, endYear);
      System.out.println("年份\t氮盈余(kgsqkm)\t磷盈余(kgsqkm)");
      for (Result result : scanner) {
        // 解析RowKey和列值
        String rowKey = Bytes.toString(result.getRow());
        int year = Bytes.toInt(result.getValue(CF_BASIC, COL_YEAR));
        // 容错：避免空值导致NPE
        double nSurplus = result.containsColumn(CF_SURPLUS, COL_N_AG_SURPLUS)
                ? Bytes.toDouble(result.getValue(CF_SURPLUS, COL_N_AG_SURPLUS)) : 0.0;
        double pSurplus = result.containsColumn(CF_SURPLUS, COL_P_AG_SURPLUS)
                ? Bytes.toDouble(result.getValue(CF_SURPLUS, COL_P_AG_SURPLUS)) : 0.0;

        // 输出结果（可替换为写入MySQL/返回前端）
        System.out.printf("%d\t%.2f\t\t%.2f%n", year, nSurplus, pSurplus);
      }
      scanner.close(); // 关闭扫描器释放资源
    }
  }

  // 测试主方法
  public static void main(String[] args) {
    try {
      // 1. 初始化表（仅首次执行）
      createTable();

      // 2. 模拟写入测试数据
      putWatershedData("38001", 2010, 2500.5, 120.5, 80.2, 300.1, 50.3);
      putWatershedData("38001", 2015, 2500.5, 135.7, 85.9, 320.5, 55.8);
      putWatershedData("38001", 2020, 2500.5, 142.3, 90.1, 350.2, 60.5);

      // 3. 查询38001流域2010-2020年氮磷盈余趋势
      querySurplusTrend("38001", 2010, 2020);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}