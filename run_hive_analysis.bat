@echo off

REM Hive分析脚本 - Windows批处理版本
REM 支持版本控制和增量更新
REM 用途：在虚拟机中直接执行Hive统计分析，避免IDE远程调用
REM 作者：系统生成
REM 日期：2025-12-15

REM 配置参数
set HIVE_SQL_FILE=hive统计分析.sql
set LOG_FILE=hive_analysis_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set HIVE_HOME=C:\hive
set HADOOP_HOME=C:\hadoop

REM 日志函数
:log
    echo %date% %time% %~1 >> %LOG_FILE%
    echo %date% %time% %~1
    goto :eof

REM 检查命令是否存在
:check_command
    where %~1 >nul 2>nul
    if %errorlevel% neq 0 (
        call :log "错误: %~1 命令未找到"
        exit /b 1
    )
    goto :eof

REM 检查Hive服务是否运行
:check_hive_service
    call :log "检查Hive服务状态..."
    %HIVE_HOME%\bin\beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;" >nul 2>nul
    if %errorlevel% neq 0 (
        call :log "错误: Hive服务未运行，请先启动Hive服务"
        call :log "提示: 可以使用以下命令启动Hive服务:"
        call :log "  %HIVE_HOME%\bin\hive --service metastore ^&"
        call :log "  %HIVE_HOME%\bin\hive --service hiveserver2 ^&"
        exit /b 1
    )
    call :log "Hive服务已运行"
    goto :eof

REM 检查Hadoop服务是否运行
:check_hadoop_service
    call :log "检查Hadoop服务状态..."
    %HADOOP_HOME%\bin\hdfs dfs -ls / >nul 2>nul
    if %errorlevel% neq 0 (
        call :log "错误: Hadoop服务未运行，请先启动Hadoop服务"
        call :log "提示: 可以使用以下命令启动Hadoop服务:"
        call :log "  %HADOOP_HOME%\sbin\start-all.cmd"
        exit /b 1
    )
    call :log "Hadoop服务已运行"
    goto :eof

REM 检查SQL文件是否存在
:check_sql_file
    if not exist "%HIVE_SQL_FILE%" (
        call :log "错误: SQL文件 %HIVE_SQL_FILE% 不存在"
        exit /b 1
    )
    call :log "找到SQL文件: %HIVE_SQL_FILE%"
    goto :eof

REM 执行Hive分析
:run_hive_analysis
    call :log "开始执行Hive分析..."
    
    REM 使用Hive命令执行SQL文件
    %HIVE_HOME%\bin\hive -f "%HIVE_SQL_FILE%" >> %LOG_FILE% 2>&1
    if %errorlevel% equ 0 (
        call :log "Hive分析执行成功"
        
        REM 显示最新版本信息
        call :log "最新版本信息:"
        %HIVE_HOME%\bin\hive -e "USE crime_analysis; SELECT * FROM analysis_version ORDER BY version_id DESC LIMIT 10;" >> %LOG_FILE% 2>&1
        %HIVE_HOME%\bin\hive -e "USE crime_analysis; SELECT * FROM analysis_version ORDER BY version_id DESC LIMIT 10;"
        
        exit /b 0
    ) else (
        call :log "Hive分析执行失败，请查看日志文件: %LOG_FILE%"
        exit /b 1
    )

REM 主函数
:main
    call :log "=== Hive分析脚本启动 ==="
    
    REM 检查必要命令
    call :check_command "hive"
    call :check_command "hdfs"
    
    REM 检查服务状态
    call :check_hadoop_service
    if %errorlevel% neq 0 exit /b 1
    
    call :check_hive_service
    if %errorlevel% neq 0 exit /b 1
    
    REM 检查SQL文件
    call :check_sql_file
    if %errorlevel% neq 0 exit /b 1
    
    REM 执行分析
    call :run_hive_analysis
    if %errorlevel% equ 0 (
        call :log "=== Hive分析脚本执行成功 ==="
        exit /b 0
    ) else (
        call :log "=== Hive分析脚本执行失败 ==="
        exit /b 1
    )

REM 执行主函数
call :main
