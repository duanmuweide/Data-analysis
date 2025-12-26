#!/bin/bash
# 每日高风险犯罪数据导出脚本
# 执行Hive查询并将结果导出到MySQL（不使用Sqoop）

# 配置参数
date=$(date +"%Y-%m-%d")
tmp_dir="/tmp/crime_export_$date"
hive_script="/home/master/Data-analysis/hive/queries/daily_high_risk_crime.sql"
export_csv="$tmp_dir/high_risk_crimes.csv"

# MySQL配置
mysql_host="localhost"
mysql_port="3306"
mysql_user="root"
mysql_password="root"
mysql_db="crime_analysis"
mysql_table="daily_high_risk_crimes"

# 创建临时目录
mkdir -p $tmp_dir

echo "=== 开始执行每日高风险犯罪数据导出 ($date) ==="

# 1. 执行Hive查询，将结果导出为CSV
# 关键修改: 使用 -f 选项来执行包含 ADD JAR 的脚本
echo "1. 执行Hive查询..."
hive -f "$hive_script" > "$export_csv" 2>&1

if [ $? -ne 0 ]; then
    echo "错误：Hive查询执行失败！"
    cat "$export_csv"
    exit 1
fi

# 2. 将数据导入MySQL
echo "2. 将数据导入MySQL..."

# 创建临时SQL文件
sql_file="$tmp_dir/import.sql"
echo "USE $mysql_db;" > "$sql_file"
echo "DELETE FROM $mysql_table WHERE export_date = '$date';" >> "$sql_file"

# 生成INSERT语句 (字段顺序必须与新SQL的SELECT顺序完全一致!)
while IFS=$'\t' read -r city state zip_code high_risk_count avg_victims latest_incident center_lat center_lon total_incidents high_risk_percentage; do
    # 跳过空行
    [ -z "$city" ] && continue
    
    # 处理NULL值和引号
    city=$(echo "$city" | sed "s/'/''/g")
    state=$(echo "$state" | sed "s/'/''/g")
    zip_code=$(echo "$zip_code" | sed "s/'/''/g")
    
    # 构建INSERT语句
    echo "INSERT INTO $mysql_table (city, state, zip_code, high_risk_count, avg_victims, latest_incident, center_lat, center_lon, total_incidents, high_risk_percentage, export_date) VALUES ('$city', '$state', '$zip_code', $high_risk_count, $avg_victims, '$latest_incident', $center_lat, $center_lon, $total_incidents, $high_risk_percentage, '$date');" >> "$sql_file"
done < "$export_csv"

# 执行SQL导入
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_password" < "$sql_file" 2>&1

if [ $? -ne 0 ]; then
    echo "错误：MySQL导入失败！"
    exit 1
fi

echo "✓ 数据导入成功！"

# 3. 清理临时文件
echo "3. 清理临时文件..."
rm -rf "$tmp_dir"

echo "=== 每日高风险犯罪数据导出完成 ($date) ==="