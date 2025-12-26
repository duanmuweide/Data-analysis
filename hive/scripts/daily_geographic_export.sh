#!/bin/bash

echo "=== 开始执行地理分布分析数据导出 ($date) ==="

date=$(date +"%Y-%m-%d")
tmp_dir="/tmp/geographic_export_$date"
hive_script="/home/master/Data-analysis/hive/queries/daily_geographic_distribution.sql"
export_csv="$tmp_dir/geographic_distribution.csv"

mysql_host="localhost"
mysql_port="3306"
mysql_user="root"
mysql_password="root"
mysql_db="crime_analysis"
mysql_table="geographic_distribution"

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

while IFS=$'\t' read -r state state_crimes state_victims cities_affected zip_codes_affected avg_victims_per_crime victim_stddev state_crime_rank state_victim_rank state_city_count_rank top_city_in_state top_city_crimes overall_top_city overall_top_city_crimes highest_crime_state highest_state_crimes crime_level; do
    state=$(echo "$state" | tr -d '\r' | xargs)
    state_crimes=$(echo "$state_crimes" | tr -d '\r' | xargs)
    state_victims=$(echo "$state_victims" | tr -d '\r' | xargs)
    cities_affected=$(echo "$cities_affected" | tr -d '\r' | xargs)
    zip_codes_affected=$(echo "$zip_codes_affected" | tr -d '\r' | xargs)
    avg_victims_per_crime=$(echo "$avg_victims_per_crime" | tr -d '\r' | xargs)
    victim_stddev=$(echo "$victim_stddev" | tr -d '\r' | xargs)
    state_crime_rank=$(echo "$state_crime_rank" | tr -d '\r' | xargs)
    state_victim_rank=$(echo "$state_victim_rank" | tr -d '\r' | xargs)
    state_city_count_rank=$(echo "$state_city_count_rank" | tr -d '\r' | xargs)
    top_city_in_state=$(echo "$top_city_in_state" | tr -d '\r' | xargs)
    top_city_crimes=$(echo "$top_city_crimes" | tr -d '\r' | xargs)
    overall_top_city=$(echo "$overall_top_city" | tr -d '\r' | xargs)
    overall_top_city_crimes=$(echo "$overall_top_city_crimes" | tr -d '\r' | xargs)
    highest_crime_state=$(echo "$highest_crime_state" | tr -d '\r' | xargs)
    highest_state_crimes=$(echo "$highest_state_crimes" | tr -d '\r' | xargs)
    crime_level=$(echo "$crime_level" | tr -d '\r' | xargs)

    if [ -n "$state" ]; then
        echo "INSERT INTO $mysql_table (state, state_crimes, state_victims, cities_affected, zip_codes_affected, avg_victims_per_crime, victim_stddev, state_crime_rank, state_victim_rank, state_city_count_rank, top_city_in_state, top_city_crimes, overall_top_city, overall_top_city_crimes, highest_crime_state, highest_state_crimes, crime_level, export_date) VALUES ('$state', '$state_crimes', '$state_victims', '$cities_affected', '$zip_codes_affected', '$avg_victims_per_crime', '$victim_stddev', '$state_crime_rank', '$state_victim_rank', '$state_city_count_rank', '$top_city_in_state', '$top_city_crimes', '$overall_top_city', '$overall_top_city_crimes', '$highest_crime_state', '$highest_state_crimes', '$crime_level', '$date');" >> "$import_sql"
    fi
done < "$export_csv"

echo "4. 导入MySQL..."
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_password" < "$import_sql" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ 数据导入成功！"
    echo "5. 清理临时文件..."
    rm -rf "$tmp_dir"
    echo "=== 地理分布分析数据导出完成 ==="
else
    echo "✗ MySQL导入失败！"
    exit 1
fi
