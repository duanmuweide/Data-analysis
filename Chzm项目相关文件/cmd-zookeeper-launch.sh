#!/bin/bash

# 定义 ZooKeeper 安装路径
ZK_HOME="/usr/local/zookeeper-3.6.1"

echo "--- 正在启动 ZooKeeper ---"

# 执行启动脚本
"$ZK_HOME/bin/zkServer.sh" start

echo "--- ZooKeeper 启动脚本执行完毕 ---"