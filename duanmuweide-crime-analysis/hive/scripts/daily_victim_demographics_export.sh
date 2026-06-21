#!/bin/bash

echo "=== 开始执行受害者人口统计数据导出 ($date) ==="

date=$(date +"%Y-%m-%d")
tmp_dir="/tmp/victim_demographics_export_$date"
hive_script="/home/master/Data-analysis/hive/queries/daily_victim_demographics.sql"
export_csv="$tmp_dir/victim_demographics.csv"

mysql_host="localhost"
mysql_port="3306"
mysql_user="root"
mysql_password="root"
mysql_db="crime_analysis"
mysql_table="victim_demographics"

mkdir -p "$tmp_dir"

echo "1. 执行Hive查询..."
hive -f "$hive_script" > "$export_csv" 2>&1

if [ $? -ne 0 ]; then
    echo "Hive查询执行失败！"
    cat "$export_csv"
    exit 1
fi

echo "2. 处理CSV数据..."
sed -i '1d' "$export_csv"
sed -i 's/NULL//g' "$export_csv"

echo "3. 生成MySQL导入SQL..."
import_sql="$tmp_dir/import.sql"
echo "USE $mysql_db;" > "$import_sql"
echo "DELETE FROM $mysql_table WHERE export_date = '$date';" >> "$import_sql"

while IFS=$'\t' read -r victim_category incident_count total_victims avg_victims min_victims max_victims category_percentage victim_percentage highest_victim_state highest_state_avg_victims overall_avg_victims overall_min_victims overall_max_victims overall_median_victims overall_90th_percentile; do
    victim_category=$(echo "$victim_category" | tr -d '\r' | xargs)
    incident_count=$(echo "$incident_count" | tr -d '\r' | xargs)
    total_victims=$(echo "$total_victims" | tr -d '\r' | xargs)
    avg_victims=$(echo "$avg_victims" | tr -d '\r' | xargs)
    min_victims=$(echo "$min_victims" | tr -d '\r' | xargs)
    max_victims=$(echo "$max_victims" | tr -d '\r' | xargs)
    category_percentage=$(echo "$category_percentage" | tr -d '\r' | xargs)
    victim_percentage=$(echo "$victim_percentage" | tr -d '\r' | xargs)
    highest_victim_state=$(echo "$highest_victim_state" | tr -d '\r' | xargs)
    highest_state_avg_victims=$(echo "$highest_state_avg_victims" | tr -d '\r' | xargs)
    overall_avg_victims=$(echo "$overall_avg_victims" | tr -d '\r' | xargs)
    overall_min_victims=$(echo "$overall_min_victims" | tr -d '\r' | xargs)
    overall_max_victims=$(echo "$overall_max_victims" | tr -d '\r' | xargs)
    overall_median_victims=$(echo "$overall_median_victims" | tr -d '\r' | xargs)
    overall_90th_percentile=$(echo "$overall_90th_percentile" | tr -d '\r' | xargs)

    if [ -n "$victim_category" ]; then
        echo "INSERT INTO $mysql_table (victim_category, incident_count, total_victims, avg_victims, min_victims, max_victims, category_percentage, victim_percentage, highest_victim_state, highest_state_avg_victims, overall_avg_victims, overall_min_victims, overall_max_victims, overall_median_victims, overall_90th_percentile, export_date) VALUES ('$victim_category', '$incident_count', '$total_victims', '$avg_victims', '$min_victims', '$max_victims', '$category_percentage', '$victim_percentage', '$highest_victim_state', '$highest_state_avg_victims', '$overall_avg_victims', '$overall_min_victims', '$overall_max_victims', '$overall_median_victims', '$overall_90th_percentile', '$date');" >> "$import_sql"
    fi
done < "$export_csv"

echo "4. 导入MySQL..."
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_password" < "$import_sql" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ 数据导入成功！"
    echo "5. 清理临时文件..."
    rm -rf "$tmp_dir"
    echo "=== 受害者人口统计数据导出完成 ==="
else
    echo "✗ MySQL导入失败！"
    exit 1
fi
