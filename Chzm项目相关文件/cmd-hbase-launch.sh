#!/bin/bash

# 定义 HBase 安装路径
HBASE_HOME="/usr/local/hbase-2.2.4"

echo "--- 正在启动 HBase ---"

# 执行启动脚本
"$HBASE_HOME/bin/start-hbase.sh"

echo "--- HBase 启动脚本执行完毕 ---"