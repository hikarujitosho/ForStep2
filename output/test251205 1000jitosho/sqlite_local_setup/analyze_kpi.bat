@echo off
chcp 65001 > nul
REM ========================================
REM KPI分析パッケージ実行スクリプト
REM ========================================

echo.
echo ========================================
echo   KPI分析パッケージ
echo ========================================
echo.
echo 1. データ更新のみ
echo 2. データ更新 + レポート生成
echo 3. データ更新 + レポート生成 + CSVエクスポート（推奨）
echo 4. レポート生成のみ
echo 5. CSVエクスポートのみ
echo.
set /p choice="選択してください (1-5): "

cd /d "%~dp0scripts"

if "%choice%"=="1" goto update_only
if "%choice%"=="2" goto update_and_report
if "%choice%"=="3" goto full_package
if "%choice%"=="4" goto report_only
if "%choice%"=="5" goto export_only

:update_only
echo.
echo [ステップ1/1] データ更新中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe load_bronze_to_silver.py
if %errorlevel% neq 0 goto error
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe calculate_gold_kpis.py
if %errorlevel% neq 0 goto error
goto success

:update_and_report
echo.
echo [ステップ1/2] データ更新中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe load_bronze_to_silver.py
if %errorlevel% neq 0 goto error
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe calculate_gold_kpis.py
if %errorlevel% neq 0 goto error

echo.
echo [ステップ2/2] レポート生成中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe generate_kpi_report.py
if %errorlevel% neq 0 goto error
goto success

:full_package
echo.
echo [ステップ1/3] データ更新中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe load_bronze_to_silver.py
if %errorlevel% neq 0 goto error
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe calculate_gold_kpis.py
if %errorlevel% neq 0 goto error

echo.
echo [ステップ2/3] レポート生成中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe generate_kpi_report.py
if %errorlevel% neq 0 goto error

echo.
echo [ステップ3/3] CSVエクスポート中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe export_dashboard_data.py
if %errorlevel% neq 0 goto error
goto success

:report_only
echo.
echo [ステップ1/1] レポート生成中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe generate_kpi_report.py
if %errorlevel% neq 0 goto error
goto success

:export_only
echo.
echo [ステップ1/1] CSVエクスポート中...
C:/Users/PC/dev/ForStep2/data/Bronze/pre24/.venv/Scripts/python.exe export_dashboard_data.py
if %errorlevel% neq 0 goto error
goto success

:error
echo.
echo ❌ エラーが発生しました
echo.
pause
exit /b 1

:success
echo.
echo ========================================
echo   完了しました！
echo ========================================
echo.
echo 出力ファイル:
echo   - データベース: ..\database\analytics.db
echo   - レポート: ..\reports\KPI_monitoring_report_*.md
echo   - CSVデータ: ..\exports\[タイムスタンプ]\*.csv
echo.
echo 次のステップ:
echo   - Markdownレポートを開いて分析結果を確認
echo   - CSVデータをExcelやPower BIで可視化
echo.
pause
