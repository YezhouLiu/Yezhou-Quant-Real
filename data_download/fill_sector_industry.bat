@echo off
chcp 65001 >nul
cd /d %~dp0..

echo.
echo ============================================================
echo 填充 sector/industry 数据（yfinance）
echo ============================================================
echo.

python data_download\update\fill_sector_industry_yfinance.py

echo.
echo ============================================================
echo 完成
echo ============================================================
echo.

pause
