@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
echo ========================================
echo 运行数据下载测试
echo ========================================
echo.

cd /d %~dp0..

set TOTAL_TESTS=0
set PASSED_TESTS=0
set FAILED_TESTS=0

echo 扫描 tests\data_download\ 下的所有测试文件...
echo ----------------------------------------
echo.

for %%f in (tests\data_download\test_*.py) do (
    set /a TOTAL_TESTS+=1
    echo [测试 !TOTAL_TESTS!] %%~nxf
    echo ----------------------------------------
    python %%f
    if errorlevel 1 (
        set /a FAILED_TESTS+=1
        echo [X] %%~nxf 失败
    ) else (
        set /a PASSED_TESTS+=1
        echo [OK] %%~nxf 通过
    )
    echo.
)

echo ========================================
echo 测试汇总
echo ----------------------------------------
echo Total: !TOTAL_TESTS! files
echo Passed: !PASSED_TESTS!
echo Failed: !FAILED_TESTS!
echo ========================================

if !FAILED_TESTS! gtr 0 (
    echo [X] 有测试失败
    exit /b 1
) else (
    echo [OK] 所有测试通过！
)

pause
