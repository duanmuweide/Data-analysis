#!/bin/bash

echo "=== 开始执行时间模式分析数据导出 ($date) ==="

date=$(date +"%Y-%m-%d")
tmp_dir="/tmp/time_pattern_export_$date"
hive_script="/home/master/Data-analysis/hive/queries/daily_time_pattern_analysis.sql"
export_csv="$tmp_dir/time_pattern_analysis.csv"

mysql_host="localhost"
mysql_port="3306"
mysql_user="root"
mysql_password="root"
mysql_db="crime_analysis"
mysql_table="time_pattern_analysis"

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

while IFS=$'\t' read -r hour_of_day hourly_crimes hourly_victims avg_victims_per_hour states_affected hour_crime_rank hour_victim_rank peak_day_name peak_day_crimes peak_quarter peak_quarter_crimes time_period crime_intensity; do
    hour_of_day=$(echo "$hour_of_day" | tr -d '\r' | xargs)
    hourly_crimes=$(echo "$hourly_crimes" | tr -d '\r' | xargs)
    hourly_victims=$(echo "$hourly_victims" | tr -d '\r' | xargs)
    avg_victims_per_hour=$(echo "$avg_victims_per_hour" | tr -d '\r' | xargs)
    states_affected=$(echo "$states_affected" | tr -d '\r' | xargs)
    hour_crime_rank=$(echo "$hour_crime_rank" | tr -d '\r' | xargs)
    hour_victim_rank=$(echo "$hour_victim_rank" | tr -d '\r' | xargs)
    peak_day_name=$(echo "$peak_day_name" | tr -d '\r' | xargs)
    peak_day_crimes=$(echo "$peak_day_crimes" | tr -d '\r' | xargs)
    peak_quarter=$(echo "$peak_quarter" | tr -d '\r' | xargs)
    peak_quarter_crimes=$(echo "$peak_quarter_crimes" | tr -d '\r' | xargs)
    time_period=$(echo "$time_period" | tr -d '\r' | xargs)
    crime_intensity=$(echo "$crime_intensity" | tr -d '\r' | xargs)

    if [ -n "$hour_of_day" ]; then
        echo "INSERT INTO $mysql_table (hour_of_day, hourly_crimes, hourly_victims, avg_victims_per_hour, states_affected, hour_crime_rank, hour_victim_rank, peak_day_name, peak_day_crimes, peak_quarter, peak_quarter_crimes, time_period, crime_intensity, export_date) VALUES ('$hour_of_day', '$hourly_crimes', '$hourly_victims', '$avg_victims_per_hour', '$states_affected', '$hour_crime_rank', '$hour_victim_rank', '$peak_day_name', '$peak_day_crimes', '$peak_quarter', '$peak_quarter_crimes', '$time_period', '$crime_intensity', '$date');" >> "$import_sql"
    fi
done < "$export_csv"

echo "4. 导入MySQL..."
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_password" < "$import_sql" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ 数据导入成功！"
    echo "5. 清理临时文件..."
    rm -rf "$tmp_dir"
    echo "=== 时间模式分析数据导出完成 ==="
else
    echo "✗ MySQL导入失败！"
    exit 1
fi
