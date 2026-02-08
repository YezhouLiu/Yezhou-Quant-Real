@echo off
chcp 65001 >nul
echo ========================================
echo 运行数据库测试套件
echo ========================================
echo.

cd /d %~dp0..

echo [1/3] 检查 pytest 是否安装...
python -m pytest --version >nul 2>&1
if errorlevel 1 (
    echo [✖] pytest 未安装，正在安装...
    pip install pytest
    if errorlevel 1 (
        echo [✖] pytest 安装失败，请手动安装: pip install pytest
        pause
        exit /b 1
    )
    echo [✔] pytest 安装成功
) else (
    echo [✔] pytest 已安装
)
echo.

echo [2/3] 运行所有数据库测试...
echo ----------------------------------------
python -m pytest tests/engine/ -v --tb=short
set TEST_RESULT=%errorlevel%
echo.

echo [3/3] 测试结果汇总
echo ----------------------------------------
if %TEST_RESULT%==0 (
    echo [✔] 所有测试通过！
    echo.
    echo 详细统计：
    python -m pytest tests/engine/ -v --tb=line -q
) else (
    echo [✖] 部分测试失败，请查看上方详情
)
echo.
echo ========================================

pause
