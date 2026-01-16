@echo off
REM ================================================================
REM 生成可交易候选股票池 CSV
REM ================================================================
REM 
REM 功能：爬取 S&P、Russell、NASDAQ 等指数成分股
REM 输出：csv/tradable_candidates.csv
REM 
REM 数据来源：
REM   - Wikipedia (S&P 500/400/600, NASDAQ-100)
REM   - iShares ETF (Russell 1000/2000)
REM 
REM Author: YezhouLiu
REM Date: 2026-01-16
REM ================================================================

REM 设置 UTF-8 编码
chcp 65001 >nul

echo.
echo ============================================================
echo 开始生成可交易候选股票池
echo ============================================================
echo.
echo 数据来源：
echo   - S^&P 500/400/600 (Wikipedia)
echo   - Russell 1000/2000 (iShares ETF)
echo   - NASDAQ-100 (Wikipedia)
echo.
echo 注意：此过程需要网络连接，大约 30-60 秒
echo.

REM 切换到项目根目录
cd /d "%~dp0\.."

REM 运行 Python 脚本
python data_download\input\tradable_candidates.py

echo.
echo ============================================================
echo 生成完成，请检查 csv\tradable_candidates.csv
echo ============================================================
echo.
echo 下一步:
echo   1. 用 Excel/VSCode 打开 CSV 检查数据
echo   2. 可手动添加 OpenAI SpaceX 等股票
echo   3. 运行导入脚本将数据导入数据库
echo.

pause
