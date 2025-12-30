# HBase 表结构设计方案

## 一、业务背景

本方案针对犯罪数据管理系统，需要存储和分析大量的犯罪记录数据，包括案件基本信息、受害者信息、地理位置信息等。

## 二、表结构设计

### 2.1 主表：crime_incidents

**表名**：`crime_incidents`

**RowKey设计**：`{state}_{city}_{incident_id}_{timestamp}`

**列族设计**：

| 列族 | 列名 | 数据类型 | 说明 |
|------|------|----------|------|
| cf | incident_id | String | 案件ID（唯一标识） |
| cf | incident_type | String | 案件类型（如：暴力犯罪、财产犯罪等） |
| cf | state | String | 州名 |
| cf | city | String | 城市名 |
| cf | zip_code | String | 邮编 |
| cf | victims | Int | 受害者数量 |
| cf | timestamp | String | 案件发生时间（格式：YYYY-MM-DD HH:mm:ss） |
| cf | latitude | Double | 纬度 |
| cf | longitude | Double | 经度 |
| cf | description | String | 案件描述 |
| cf | status | String | 案件状态（如：已结案、调查中等） |

### 2.2 辅助表：crime_index_by_date

**表名**：`crime_index_by_date`

**RowKey设计**：`{timestamp}_{state}_{city}_{incident_id}`

**列族设计**：

| 列族 | 列名 | 数据类型 | 说明 |
|------|------|----------|------|
| cf | incident_id | String | 案件ID（引用主表） |

### 2.3 辅助表：crime_index_by_state

**表名**：`crime_index_by_state`

**RowKey设计**：`{state}_{city}_{timestamp}_{incident_id}`

**列族设计**：

| 列族 | 列名 | 数据类型 | 说明 |
|------|------|----------|------|
| cf | incident_id | String | 案件ID（引用主表） |

## 三、RowKey设计详解

### 3.1 主表RowKey：`{state}_{city}_{incident_id}_{timestamp}`

#### 设计原则

1. **长度控制**：RowKey总长度控制在100字节以内
2. **散列性**：使用state和city作为前缀，确保数据均匀分布
3. **查询友好**：支持按州、城市、案件ID、时间等多维度查询
4. **唯一性**：incident_id确保唯一性

#### 各部分说明

| 部分 | 说明 | 示例 |
|------|------|------|
| state | 州名（2-3字符） | CA, NY, TX |
| city | 城市名（10-20字符） | Los Angeles, New York |
| incident_id | 案件ID（8-12字符） | 20231227001 |
| timestamp | 时间戳（14字符） | 20231227143000 |

**示例RowKey**：
```
CA_Los Angeles_20231227001_20231227143000
NY_New York_20231227002_20231227143500
TX_Houston_20231227003_20231227144000
```

### 3.2 辅助表RowKey设计

#### 按时间索引：`{timestamp}_{state}_{city}_{incident_id}`
- 适用于按时间范围查询
- 时间戳在前，支持Scan时的时间范围过滤

#### 按州索引：`{state}_{city}_{timestamp}_{incident_id}`
- 适用于按州、城市查询
- 与主表RowKey类似，但时间戳位置不同，优化查询性能

## 四、避免数据热点

### 4.1 数据热点问题

**定义**：大量数据集中在某个RegionServer上，导致该RegionServer负载过高，影响整体性能。

**原因**：
1. RowKey设计不合理，导致数据倾斜
2. 某些州或城市的案件数量远超其他地区
3. 时间序列数据导致新数据集中在最新时间

### 4.2 解决方案

#### 方案1：RowKey加盐（Salting）

**原理**：在RowKey前添加随机前缀，将数据分散到多个Region

**实现**：
```
RowKey = {salt}_{state}_{city}_{incident_id}_{timestamp}

其中salt = hash(state) % N
N = Region数量（如：10）
```

**示例**：
```
3_CA_Los Angeles_20231227001_20231227143000
7_NY_New York_20231227002_20231227143500
2_TX_Houston_20231227003_20231227144000
```

**优点**：
- 数据均匀分布
- 避免热点Region

**缺点**：
- 查询时需要遍历所有可能的salt值
- 增加查询复杂度



### 4.3 推荐方案

针对犯罪数据的特点，推荐使用**方案1（RowKey加盐）**，理由如下：

1. 犯罪数据查询模式主要是按州、城市查询，可以接受查询时遍历salt值
2. 数据写入量大，需要保证写入性能
3. 可以通过预分区（Pre-splitting）进一步优化

**实现细节**：

```bash
# 1. 预创建Region（假设10个Region）
create 'crime_incidents', 'cf', {SPLITS => ['1_', '2_', '3_', '4_', '5_', '6_', '7_', '8_', '9_']}

# 2. 写入数据时计算salt
salt = hash(state) % 10
rowkey = salt + "_" + state + "_" + city + "_" + incident_id + "_" + timestamp

# 3. 查询时遍历所有salt值
for i in range(10):
    scan 'crime_incidents', {STARTROW => i + '_' + state, STOPROW => i + '_' + state + '~'}
```

## 五、避免数据倾斜

### 5.1 数据倾斜问题

**定义**：某些Region的数据量远大于其他Region，导致负载不均衡。

**原因**：
1. 某些州或城市的案件数量远超其他地区
2. 某些时间段的案件数量激增
3. RowKey设计不合理

### 5.2 解决方案

#### 方案1：预分区（Pre-splitting）

**原理**：根据数据分布特征，预先创建多个Region

**实现**：
```bash
# 基于州名预分区
create 'crime_incidents', 'cf', {SPLITS => ['AK_', 'AL_', 'AR_', 'AZ_', 'CA_', 'CO_', 'CT_', 'DE_', 'FL_', 'GA_']}

# 基于哈希值预分区
create 'crime_incidents', 'cf', {SPLITS => ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']}
```

**优点**：
- 数据分布可控
- 避免Region自动分裂带来的性能影响

**缺点**：
- 需要预先了解数据分布
- 数据分布变化时需要手动调整

#### 方案2：动态Region调整

**原理**：监控Region大小，自动进行Region拆分或合并

**配置**：
```xml
<property>
  <name>hbase.hregion.max.filesize</name>
  <value>10737418240</value>
</property>
<property>
  <name>hbase.regionserver.region.split.limit</name>
  <value>1000</value>
</property>
```

**优点**：
- 自动适应数据分布变化
- 无需手动干预

**缺点**：
- Region分裂期间性能下降
- 可能产生大量小Region

#### 方案3：数据分层存储

**原理**：将热点数据和历史数据分开存储

**实现**：
- 热点数据（最近30天）：存储在SSD存储的Region
- 历史数据（30天前）：存储在HDD存储的Region

**配置**：
```bash
# 创建不同存储策略的表
create 'crime_incidents_hot', 'cf'
create 'crime_incidents_cold', 'cf'

# 定期将冷数据迁移到冷表
```

**优点**：
- 优化查询性能
- 降低存储成本

**缺点**：
- 需要定期维护
- 增加系统复杂度

### 5.3 推荐方案

针对犯罪数据的特点，推荐使用**方案1（预分区）+ 方案2（动态Region调整）**的组合方案：

1. **初始阶段**：基于州名预分区，确保每个州的数据分布在不同Region
2. **运行阶段**：启用动态Region调整，自动处理数据增长
3. **监控阶段**：定期监控Region大小，手动调整异常Region

**实现细节**：

```bash
# 1. 基于州名预分区（假设有50个州）
splits = ['AK_', 'AL_', 'AR_', 'AZ_', 'CA_', 'CO_', 'CT_', 'DE_', 'FL_', 'GA_',
          'HI_', 'IA_', 'ID_', 'IL_', 'IN_', 'KS_', 'KY_', 'LA_', 'MA_', 'MD_',
          'ME_', 'MI_', 'MN_', 'MO_', 'MS_', 'MT_', 'NC_', 'ND_', 'NE_', 'NH_',
          'NJ_', 'NM_', 'NV_', 'NY_', 'OH_', 'OK_', 'OR_', 'PA_', 'RI_', 'SC_',
          'SD_', 'TN_', 'TX_', 'UT_', 'VA_', 'VT_', 'WA_', 'WI_', 'WV_', 'WY_']
create 'crime_incidents', 'cf', {SPLITS => splits}

# 2. 配置动态Region调整
# 在hbase-site.xml中配置
hbase.hregion.max.filesize = 10GB
hbase.regionserver.region.split.limit = 1000

# 3. 定期监控Region大小
# 使用HBase Shell或API监控
```

## 六、性能优化建议

### 6.1 列族设计

1. **列族数量**：建议1-2个列族，避免过多列族
2. **列族名称**：使用短名称（如：cf），减少存储开销
3. **版本管理**：设置合理的版本数量，避免数据膨胀

```bash
# 创建表时指定列族属性
create 'crime_incidents', {NAME => 'cf', VERSIONS => 3, TTL => 2592000}
```

### 6.2 缓存配置

```bash
# 启用BlockCache
alter 'crime_incidents', {METHOD => 'table_att', 'DATA_BLOCK_ENCODING' => 'FAST_DIFF'}

# 配置MemStore
alter 'crime_incidents', {METHOD => 'table_att', 'MEMSTORE_FLUSHSIZE' => 134217728}
```

### 6.3 压缩配置

```bash
# 启用压缩
alter 'crime_incidents', {NAME => 'cf', COMPRESSION => 'SNAPPY'}
```

## 七、查询优化

### 7.1 按州查询

```bash
# 查询某个州的所有案件
scan 'crime_incidents', {STARTROW => 'CA_', STOPROW => 'CA_~'}
```

### 7.2 按城市查询

```bash
# 查询某个城市的所有案件
scan 'crime_incidents', {STARTROW => 'CA_Los Angeles_', STOPROW => 'CA_Los Angeles~'}
```

### 7.3 按时间范围查询

```bash
# 使用辅助表查询时间范围
scan 'crime_index_by_date', {STARTROW => '20231227_', STOPROW => '20231228_'}
```

### 7.4 使用过滤器

```bash
# 使用SingleColumnValueFilter
scan 'crime_incidents', {FILTER => "SingleColumnValueFilter('cf', 'victims', =, 'binary:2')"}
```

## 八、总结

本方案针对犯罪数据的特点，设计了合理的HBase表结构和RowKey，并提供了避免数据热点和数据倾斜的解决方案：

1. **表结构**：主表 + 2个辅助表，支持多维度查询
2. **RowKey设计**：`{state}_{city}_{incident_id}_{timestamp}`，支持多维度查询
3. **避免热点**：使用RowKey加盐（Salting）技术
4. **避免倾斜**：使用预分区 + 动态Region调整
5. **性能优化**：列族设计、缓存配置、压缩配置

该方案在保证查询性能的同时，有效避免了数据热点和数据倾斜问题。
