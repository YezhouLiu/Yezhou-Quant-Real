@echo off
chcp 65001 >nul
cd /d %~dp0

echo ========================================
echo 删除所有数据库表
echo ========================================
echo.

python -m database.schema.drop_tables

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
