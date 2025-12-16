#!/bin/bash

# Hive分析脚本 - 支持版本控制和增量更新
# 用途：在虚拟机中直接执行Hive统计分析，避免IDE远程调用
# 作者：系统生成
# 日期：2025-12-15

# 配置参数
HIVE_SQL_FILE="hive统计分析.sql"
LOG_FILE="hive_analysis_$(date +%Y%m%d_%H%M%S).log"
HIVE_HOME="/usr/local/hive"
HADOOP_HOME="/usr/local/hadoop"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $1" | tee -a $LOG_FILE
}

# 检查命令是否存在
check_command() {
    if ! command -v $1 &> /dev/null; then
        log "${RED}错误: $1 命令未找到${NC}"
        exit 1
    fi
}

# 检查Hive服务是否运行
check_hive_service() {
    log "${YELLOW}检查Hive服务状态...${NC}"
    if ! $HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" &> /dev/null; then
        log "${RED}错误: Hive服务未运行，请先启动Hive服务${NC}"
        log "${YELLOW}提示: 可以使用以下命令启动Hive服务:${NC}"
        log "${YELLOW}  $HIVE_HOME/bin/hive --service metastore &${NC}"
        log "${YELLOW}  $HIVE_HOME/bin/hive --service hiveserver2 &${NC}"
        exit 1
    fi
    log "${GREEN}Hive服务已运行${NC}"
}

# 检查Hadoop服务是否运行
check_hadoop_service() {
    log "${YELLOW}检查Hadoop服务状态...${NC}"
    if ! $HADOOP_HOME/bin/hdfs dfs -ls / &> /dev/null; then
        log "${RED}错误: Hadoop服务未运行，请先启动Hadoop服务${NC}"
        log "${YELLOW}提示: 可以使用以下命令启动Hadoop服务:${NC}"
        log "${YELLOW}  $HADOOP_HOME/sbin/start-all.sh${NC}"
        exit 1
    fi
    log "${GREEN}Hadoop服务已运行${NC}"
}

# 检查SQL文件是否存在
check_sql_file() {
    if [ ! -f "$HIVE_SQL_FILE" ]; then
        log "${RED}错误: SQL文件 $HIVE_SQL_FILE 不存在${NC}"
        exit 1
    fi
    log "${GREEN}找到SQL文件: $HIVE_SQL_FILE${NC}"
}

# 执行Hive分析
run_hive_analysis() {
    log "${YELLOW}开始执行Hive分析...${NC}"
    
    # 使用Hive命令执行SQL文件
    if $HIVE_HOME/bin/hive -f "$HIVE_SQL_FILE" &>> $LOG_FILE; then
        log "${GREEN}Hive分析执行成功${NC}"
        
        # 显示最新版本信息
        log "${YELLOW}最新版本信息:${NC}"
        $HIVE_HOME/bin/hive -e "USE crime_analysis; SELECT * FROM analysis_version ORDER BY version_id DESC LIMIT 10;" 2>> $LOG_FILE | tee -a $LOG_FILE
        
        return 0
    else
        log "${RED}Hive分析执行失败，请查看日志文件: $LOG_FILE${NC}"
        return 1
    fi
}

# 主函数
main() {
    log "${GREEN}=== Hive分析脚本启动 ===${NC}"
    
    # 检查必要命令
    check_command "hive"
    check_command "hdfs"
    
    # 检查服务状态
    check_hadoop_service
    check_hive_service
    
    # 检查SQL文件
    check_sql_file
    
    # 执行分析
    if run_hive_analysis; then
        log "${GREEN}=== Hive分析脚本执行成功 ===${NC}"
        exit 0
    else
        log "${RED}=== Hive分析脚本执行失败 ===${NC}"
        exit 1
    fi
}

# 执行主函数
main