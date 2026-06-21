#!/bin/bash

# 定义 HBase 安装路径
HBASE_HOME="/usr/local/hbase-2.2.4"

echo "--- 正在进入 HBase Shell ---"
"$HBASE_HOME/bin/hbase" shell