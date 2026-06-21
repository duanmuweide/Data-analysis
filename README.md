# Data Analysis — 大数据方向项目合集

> **2025 年第 5 学期 · 大数据方向项目**  
> 青岛大学 (QDU) · 三人小组合作项目

## 项目简介

本仓库包含第 5 学期大数据方向的三组数据分析项目，分别由三位成员独立完成：

| 成员 | 分析主题 | 技术栈 | 目录 |
|------|----------|--------|------|
| **duanmuweide** | 美国犯罪数据分析 | Hive + HBase + MapReduce | `duanmuweide-crime-analysis/` |
| **Czm** | 美国流域养分平衡分析 | Hive + HBase + Phoenix | `czm-watershed-analysis/` |
| **第三位成员** | 北京二手房价格分析 | Hive + HBase + JSP Web | `housing-price-analysis/` |

三个子项目共享 Hadoop 生态技术栈，但各自拥有独立的 Maven 工程、分析逻辑和数据流。

---

## 技术栈总览

| 组件 | 版本 | 用途 |
|------|------|------|
| Hadoop | 2.8.5 / 3.3.6 | 分布式存储与计算 |
| Hive | 2.3.7 | 数据仓库、SQL-on-Hadoop 分析 |
| HBase | 2.2.4 | 列式 NoSQL，实时查询与动态存储 |
| ZooKeeper | 3.6.1 | 分布式协调（HBase 依赖） |
| Phoenix | 2.2.5.1.3 (CDH) | HBase SQL 查询层（Czm 项目） |
| MySQL | 8.0.21 / 8.0.33 | 分析结果持久化存储 |
| Java | JDK 8 | 后端 API 开发 |
| Maven | - | 项目构建管理 |
| JSP / Servlet | Servlet 4.0 | 前端仪表盘（北京二手房项目） |
| Python | 3.x | 数据清洗与预处理 |

---

## 目录结构

```
Data-analysis-main/
├── README.md                                  # 本文件
├── .gitignore                                 # 忽略编译产物和数据文件
├── docs/
│   └── 项目设计要求.pdf
│
├── duanmuweide-crime-analysis/               # 美国犯罪数据分析
│   ├── pom.xml                                # Maven 构建文件
│   ├── src/                                   # Java 源码
│   │   └── main/java/com/qdu/
│   │       ├── connection/                    # 连接工具类 (HBase/Hive/MySQL)
│   │       ├── hbase/                         # HBase 数据导入
│   │       └── hive/                          # Hive UDF（风险等级分类）
│   ├── hive/                                  # HQL 脚本 + Shell
│   │   ├── hbase/                             # HBase 表结构设计
│   │   ├── load/                              # 数据加载脚本
│   │   ├── queries/                           # 5 个分析查询
│   │   ├── schema/                            # MySQL 结果表结构
│   │   └── scripts/                           # 5 个导出脚本
│   ├── crime-data/                            # Python 数据清洗
│   └── 创建Hive外部表/                        # Hive 建表 SQL
│
├── czm-watershed-analysis/                   # 美国流域养分平衡分析
│   ├── CzmDataAnalysis/                       # Maven Java 主项目
│   │   ├── pom.xml
│   │   └── src/main/java/com/qdu/
│   │       ├── connection/                    # 连接工具类
│   │       ├── connection/tool/               # Hive 最大 ID 查询
│   │       ├── service/                       # 5 个分析服务
│   │       └── service/hbase/                 # HBase 数据管道
│   ├── MyFunction/                            # 自定义 UDF
│   │   ├── CorrCoefficientUDF.java            # Pearson 相关系数
│   │   └── PollutantRiskLevelUDF.java         # 污染风险等级
│   ├── Chzm项目相关文件/                      # 配套文档与脚本
│   │   ├── *.sql                              # 6 个 MySQL 建表 SQL
│   │   ├── *.txt                              # 文档、数据字典、样例
│   │   ├── *.sh                               # HBase/ZooKeeper 启动脚本
│   │   └── *.py                               # 数据清洗脚本
│   ├── readme.txt                             # Czm 的开发笔记
│   └── 项目开发有感.txt                       # 项目总结
│
├── housing-price-analysis/                   # 北京二手房价格分析
│   ├── Data-analysis/                         # Maven Web 主项目
│   │   ├── pom.xml
│   │   └── src/main/java/com/qdu/
│   │       ├── Main.java                      # 入口
│   │       ├── HiveToHBaseHouseDataMigration.java  # Hive→HBase 数据迁移
│   │       ├── area/                          # 面积区间分析
│   │       ├── district/                      # 城区价格分析
│   │       ├── hbase/                         # 小区价格聚合（HBase）
│   │       ├── led/                           # 户型/装修/电梯分析
│   │       ├── value/                         # 性价比评分
│   │       ├── year/                          # 年代区间分析
│   │       ├── udf/                           # 5 个自定义 UDF
│   │       └── servlet/                       # LoginServlet（JSP 登录）
│   ├── 二手房数据.csv                          # 源数据
│   └── 项目数据字典.xlsx                       # 字段说明
│
└── Social-Platform/                           # 未完成的社交平台副本项目
```

---

## 子项目详解

### 1. duanmuweide — 美国犯罪数据分析

**目标**：对美国犯罪事件数据进行多维度统计分析，识别高发地区、时间规律和受害者特征。

**分析模块**（5 个 HQL 查询）：

| # | 分析内容 | 说明 |
|---|----------|------|
| Q1 | 地理分布分析 | 各城市/州的犯罪事件分布 |
| Q2 | 高风险城市识别 | 按事件数量 Top-N 排名 |
| Q3 | 月度趋势分析 | 犯罪事件的时间趋势 |
| Q4 | 时间模式分析 | 一天内不同时段的分布规律 |
| Q5 | 受害者特征分析 | 受害者年龄、性别、种族分布 |

**数据流**：
```
CSV 原始数据 → Python 清洗 → HDFS → Hive 外部表 → Hive ORC 内表
                                              ↓
   Java (CrimeHBaseImportUtil) → HBase → MySQL 结果表
   Shell 脚本 → Hive 查询 → MySQL 结果表
```

**快速开始**：
```bash
# 编译
cd duanmuweide-crime-analysis
mvn clean package

# 加载数据
cd hive/load
bash loadpath.sh

# 运行分析查询
cd hive/scripts
bash *.sh
```

---

### 2. Czm — 美国流域养分平衡分析

**目标**：对美国县域级别的氮、磷养分流动数据进行平衡分析，评估农业活动对水环境的影响。

**分析模块**（5 个 Java 服务 + HQL）：

| # | 分析名称 | 核心方法 |
|---|----------|----------|
| A1 | 流域氮/磷利用效率 Top10 | 窗口函数排名 |
| A2 | 农业氮盈余 vs 大气氮沉降相关性 | Pearson 相关系数 UDF |
| A3 | 磷污染来源按流域面积分类 | CASE 聚合 |
| A4 | 历史氮盈余 vs 当前氮排放 | CTE + 自连接 + 分类 |
| A5 | 人类活动对氮平衡的影响排名 | 加权评分 + Top20 |

**数据流**：
```
CSV (3个批次) → upload_file.sh (beeline) → Hive 临时表
       → Hive watershed_nutrient_balance (分区+分桶)
              ↓
   Java 服务 → SELECT MAX(id) → where id=? 查询当前批次
              ↓
   Hive-HBase 集成 → HBase 外部表 → Java API → MySQL
```

**快速开始**：
```bash
cd czm-watershed-analysis/CzmDataAnalysis
mvn clean package
# 先创建 MySQL 表（SQL 文件在 Chzm项目相关文件/ 中）
# 再运行 Java 服务类
```

---

### 3. 第三位成员 — 北京二手房价格分析

**目标**：对北京二手房交易数据进行多维度价格分析，提供可视化评分排名。

**分析模块**（6 个 Java 分析 Job）：

| # | 分析名称 | 说明 |
|---|----------|------|
| B1 | 面积区间价格分析 | 按 ≤50/50-90/90-130/130-170/>170 分段 |
| B2 | 城区价格分析 | 各行政区均价 + 房龄计算 UDF |
| B3 | 户型/装修/电梯分析 | 布局分类 + 装修等级 UDF |
| B4 | 年代区间分析 | 按建造年代分段交叉统计 |
| B5 | 性价比评分 | 多维度加权 CROSS JOIN 评分 |
| B6 | 小区价格聚合 | HBase 小区维度聚合查询 |

**Web 前端**：JSP 登录页 (`index.jsp` → `LoginServlet` → `welcome.jsp`)

**数据流**：
```
Hive house_info_clean_checkid
    ↓
MapReduce → HBase cjz:house_info_clean_checkid
    ↓
Java 分析 Job (带 UDF)
    ↓
Hive 结果表 (分区) → MySQL 结果表
HBase cjz:community_price_analysis → MySQL
    ↓
JSP Web 仪表盘展示
```

**快速开始**：
```bash
cd housing-price-analysis/Data-analysis
mvn clean package
# Web 项目打包为 WAR，部署到 Tomcat
```

---

## 集群环境

三个子项目运行在不同集群上，连接配置各有不同：

| 子项目 | Hive JDBC | ZooKeeper | MySQL |
|--------|-----------|-----------|-------|
| 犯罪分析 | - | `hadoop101:2181,...` | `crime_analysis` |
| 流域养分 | `master-pc:10000` | `master-pc:2181` | `american_data_analysis` |
| 二手房 | - | - | 各分析独立表 |

> ⚠️ 代码中包含硬编码的数据库用户名/密码/URL，请根据实际集群环境修改 `*ConnUtil.java` 中的连接参数。

---

## 常见问题

**Q: 为什么 connection 工具类有多套？**  
A: 三个子项目运行在不同集群上（`hadoop101` 集群 vs `master-pc` 集群），连接参数不同，故各有一份。

**Q: LoginServlet 源码在哪里？**  
A: 北京二手房项目的 `LoginServlet.java` 源码已丢失，编译产物中仅保留 `.class` 文件。如需恢复，需要反编译或重写。

**Q: 如何运行单个子项目？**  
A: 每个子项目是独立的 Maven 工程，进入对应目录后 `mvn clean package` 即可。注意先启动 Hadoop/Hive/HBase/MySQL 环境。

---

## 学期对比

| | 第 5 学期（本仓库） | 第 6 学期（当前项目） |
|------|------|------|
| 计算引擎 | Hive / MapReduce | Apache Spark |
| 存储 | HBase 2.2.4 | HDFS + MySQL |
| 开发语言 | Java + HQL | Scala 2.12 |
| 实时处理 | 无 | Spark Streaming + Kafka |
| Web 框架 | JSP / Servlet | Akka HTTP |
| 前端 | 积木报表 | ECharts |

---

> 本项目仅供学习交流使用，版权归原作者所有。
