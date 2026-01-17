@echo off
REM 下载价格数据（增量模式）
REM 
REM 功能：
REM - 从 system_state 读取上次下载位置
REM - 只下载缺失的日期范围
REM - 自动更新 system_state

echo ======================================
echo   价格数据下载（增量模式）
echo ======================================
echo.

python data_download\input\price_downloader.py

echo.
echo ======================================
echo   完成
echo ======================================
pause
