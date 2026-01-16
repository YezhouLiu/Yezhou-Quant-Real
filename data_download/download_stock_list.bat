@echo off
REM ================================================================
REM 下载 Tiingo 美股股票列表
REM ================================================================
REM 
REM 功能：从 Tiingo 官方下载 supported_tickers.zip
REM 输出：csv/all_us_stocks_listed.csv
REM 
REM Author: YezhouLiu
REM Date: 2026-01-16
REM ================================================================

REM 设置 UTF-8 编码，避免中文乱码
chcp 65001 >nul

echo.
echo ============================================================
echo 开始下载 Tiingo 美股股票列表
echo ============================================================
echo.

REM 切换到项目根目录
cd /d "%~dp0\.."

REM 运行 Python 脚本
python data_download\input\all_us_stocks.py

echo.
echo ============================================================
echo 下载完成，请检查 csv\all_us_stocks_listed.csv
echo ============================================================
echo.

pause
