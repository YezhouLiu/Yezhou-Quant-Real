@echo off
chcp 65001 >nul
cd /d %~dp0

echo ========================================
echo 开始创建数据库表结构
echo ========================================
echo.

python -m database.schema.create_tables

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo 执行成功！
    echo ========================================
) else (
    echo.
    echo ========================================
    echo 执行失败！错误码: %ERRORLEVEL%
    echo ========================================
)

echo.
pause
