#!/bin/bash

echo "=== 开始执行月度犯罪趋势分析数据导出 ($date) ==="

date=$(date +"%Y-%m-%d")
tmp_dir="/tmp/monthly_trend_export_$date"
hive_script="/home/master/Data-analysis/hive/queries/daily_monthly_trend_analysis.sql"
export_csv="$tmp_dir/monthly_crime_trends.csv"

mysql_host="localhost"
mysql_port="3306"
mysql_user="root"
mysql_password="root"
mysql_db="crime_analysis"
mysql_table="monthly_crime_trends"

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

while IFS=$'\t' read -r report_month total_crimes total_victims avg_victims_per_crime affected_cities affected_states crime_growth_rate victim_growth_rate peak_day_crimes lowest_day_crimes; do
    report_month=$(echo "$report_month" | tr -d '\r' | xargs)
    total_crimes=$(echo "$total_crimes" | tr -d '\r' | xargs)
    total_victims=$(echo "$total_victims" | tr -d '\r' | xargs)
    avg_victims_per_crime=$(echo "$avg_victims_per_crime" | tr -d '\r' | xargs)
    affected_cities=$(echo "$affected_cities" | tr -d '\r' | xargs)
    affected_states=$(echo "$affected_states" | tr -d '\r' | xargs)
    crime_growth_rate=$(echo "$crime_growth_rate" | tr -d '\r' | xargs)
    victim_growth_rate=$(echo "$victim_growth_rate" | tr -d '\r' | xargs)
    peak_day_crimes=$(echo "$peak_day_crimes" | tr -d '\r' | xargs)
    lowest_day_crimes=$(echo "$lowest_day_crimes" | tr -d '\r' | xargs)

    if [ -n "$report_month" ]; then
        echo "INSERT INTO $mysql_table (report_month, total_crimes, total_victims, avg_victims_per_crime, affected_cities, affected_states, crime_growth_rate, victim_growth_rate, peak_day_crimes, lowest_day_crimes, export_date) VALUES ('$report_month', '$total_crimes', '$total_victims', '$avg_victims_per_crime', '$affected_cities', '$affected_states', '$crime_growth_rate', '$victim_growth_rate', '$peak_day_crimes', '$lowest_day_crimes', '$date');" >> "$import_sql"
    fi
done < "$export_csv"

echo "4. 导入MySQL..."
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_password" < "$import_sql" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ 数据导入成功！"
    echo "5. 清理临时文件..."
    rm -rf "$tmp_dir"
    echo "=== 月度犯罪趋势分析数据导出完成 ==="
else
    echo "✗ MySQL导入失败！"
    exit 1
fi
