@echo off
chcp 65001 > nul
REM ========================================
REM データ更新実行スクリプト
REM Bronze(CSV) → Silver → Gold
REM ========================================

echo.
echo ========================================
echo   データ更新を開始します
echo ========================================
echo.

cd /d "%~dp0scripts"

REM ステップ1: Bronze → Silver
echo.
echo ========================================
echo   ステップ1: Bronze → Silver
echo ========================================
echo.
python load_bronze_to_silver.py

if %errorlevel% neq 0 (
    echo.
    echo ❌ エラー: Silverレイヤーの作成に失敗しました
    echo.
    pause
    exit /b 1
)

REM ステップ2: Silver → Gold (KPI計算)
echo.
echo ========================================
echo   ステップ2: Silver → Gold (KPI計算)
echo ========================================
echo.
python calculate_gold_kpis.py

if %errorlevel% neq 0 (
    echo.
    echo ❌ エラー: KPI計算に失敗しました
    echo.
    pause
    exit /b 1
)

REM 完了メッセージ
echo.
echo ========================================
echo   データ更新が完了しました！
echo ========================================
echo.
echo 更新内容:
echo   - Silverテーブル (10テーブル)
echo   - Goldテーブル (5つのKPI)
echo.
echo データベース: ..\database\analytics.db
echo.
echo 次のステップ:
echo   1. DB Browser for SQLite でデータベースを開く
echo   2. queries\フォルダ内のSQLを実行して分析
echo   3. ExcelやPower BIに接続して可視化
echo.
pause
