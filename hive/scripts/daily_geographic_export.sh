#!/bin/bash

date=$(date +"%Y-%m-%d")
echo "=== 开始执行地理分布分析数据导出 ($date) ==="

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
hive_log="$tmp_dir/hive_error.log"
hive -f "$hive_script" 2>"$hive_log" > "$export_csv"
hive_exit_code=$?

if [ $hive_exit_code -ne 0 ]; then
    echo "Hive查询执行失败！退出码: $hive_exit_code"
    echo "Hive错误日志："
    cat "$hive_log"
    echo ""
    echo "CSV文件内容（如果有）："
    if [ -f "$export_csv" ]; then
        cat "$export_csv" | head -20
    else
        echo "CSV文件未生成"
    fi
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
    # 跳过空行和标题行
    [ -z "$state" ] && continue
    [ "$state" = "state" ] && continue
    
    state=$(echo "$state" | tr -d '\r' | xargs | sed "s/'/''/g" | sed 's/\\N//g')
    state_crimes=$(echo "$state_crimes" | tr -d '\r' | xargs | sed 's/\\N//g')
    state_victims=$(echo "$state_victims" | tr -d '\r' | xargs | sed 's/\\N//g')
    cities_affected=$(echo "$cities_affected" | tr -d '\r' | xargs | sed 's/\\N//g')
    zip_codes_affected=$(echo "$zip_codes_affected" | tr -d '\r' | xargs | sed 's/\\N//g')
    avg_victims_per_crime=$(echo "$avg_victims_per_crime" | tr -d '\r' | xargs | sed 's/\\N//g')
    victim_stddev=$(echo "$victim_stddev" | tr -d '\r' | xargs | sed 's/\\N//g')
    state_crime_rank=$(echo "$state_crime_rank" | tr -d '\r' | xargs | sed 's/\\N//g')
    state_victim_rank=$(echo "$state_victim_rank" | tr -d '\r' | xargs | sed 's/\\N//g')
    state_city_count_rank=$(echo "$state_city_count_rank" | tr -d '\r' | xargs | sed 's/\\N//g')
    top_city_in_state=$(echo "$top_city_in_state" | tr -d '\r' | xargs | sed "s/'/''/g" | sed 's/\\N//g')
    top_city_crimes=$(echo "$top_city_crimes" | tr -d '\r' | xargs | sed 's/\\N//g')
    overall_top_city=$(echo "$overall_top_city" | tr -d '\r' | xargs | sed "s/'/''/g" | sed 's/\\N//g')
    overall_top_city_crimes=$(echo "$overall_top_city_crimes" | tr -d '\r' | xargs | sed 's/\\N//g')
    highest_crime_state=$(echo "$highest_crime_state" | tr -d '\r' | xargs | sed "s/'/''/g" | sed 's/\\N//g')
    highest_state_crimes=$(echo "$highest_state_crimes" | tr -d '\r' | xargs | sed 's/\\N//g')
    crime_level=$(echo "$crime_level" | tr -d '\r' | xargs | sed "s/'/''/g" | sed 's/\\N//g')
    
    # 处理NULL值
    [ -z "$state_crimes" ] && state_crimes=0
    [ -z "$state_victims" ] && state_victims=0
    [ -z "$cities_affected" ] && cities_affected=0
    [ -z "$zip_codes_affected" ] && zip_codes_affected=0
    [ -z "$avg_victims_per_crime" ] && avg_victims_per_crime=0
    [ -z "$victim_stddev" ] && victim_stddev=0
    [ -z "$state_crime_rank" ] && state_crime_rank=0
    [ -z "$state_victim_rank" ] && state_victim_rank=0
    [ -z "$state_city_count_rank" ] && state_city_count_rank=0
    [ -z "$top_city_in_state" ] && top_city_in_state="NULL"
    [ -z "$top_city_crimes" ] && top_city_crimes=0
    [ -z "$overall_top_city" ] && overall_top_city="NULL"
    [ -z "$overall_top_city_crimes" ] && overall_top_city_crimes=0
    [ -z "$highest_crime_state" ] && highest_crime_state="NULL"
    [ -z "$highest_state_crimes" ] && highest_state_crimes=0
    [ -z "$crime_level" ] && crime_level="NULL"

    if [ -n "$state" ]; then
        if [ "$top_city_in_state" = "NULL" ] || [ "$overall_top_city" = "NULL" ] || [ "$highest_crime_state" = "NULL" ] || [ "$crime_level" = "NULL" ]; then
            echo "INSERT INTO $mysql_table (state, state_crimes, state_victims, cities_affected, zip_codes_affected, avg_victims_per_crime, victim_stddev, state_crime_rank, state_victim_rank, state_city_count_rank, top_city_in_state, top_city_crimes, overall_top_city, overall_top_city_crimes, highest_crime_state, highest_state_crimes, crime_level, export_date) VALUES ('$state', $state_crimes, $state_victims, $cities_affected, $zip_codes_affected, $avg_victims_per_crime, $victim_stddev, $state_crime_rank, $state_victim_rank, $state_city_count_rank, NULL, $top_city_crimes, NULL, $overall_top_city_crimes, NULL, $highest_state_crimes, NULL, '$date');" >> "$import_sql"
        else
            echo "INSERT INTO $mysql_table (state, state_crimes, state_victims, cities_affected, zip_codes_affected, avg_victims_per_crime, victim_stddev, state_crime_rank, state_victim_rank, state_city_count_rank, top_city_in_state, top_city_crimes, overall_top_city, overall_top_city_crimes, highest_crime_state, highest_state_crimes, crime_level, export_date) VALUES ('$state', $state_crimes, $state_victims, $cities_affected, $zip_codes_affected, $avg_victims_per_crime, $victim_stddev, $state_crime_rank, $state_victim_rank, $state_city_count_rank, '$top_city_in_state', $top_city_crimes, '$overall_top_city', $overall_top_city_crimes, '$highest_crime_state', $highest_state_crimes, '$crime_level', '$date');" >> "$import_sql"
        fi
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
