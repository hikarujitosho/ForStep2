@echo off
chcp 65001 > nul
REM ========================================
REM 環境セットアップスクリプト
REM ========================================

echo.
echo ========================================
echo   環境セットアップを開始します
echo ========================================
echo.

REM Pythonのバージョンチェック
echo [1/3] Pythonのバージョンを確認中...
python --version > nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo ❌ エラー: Pythonがインストールされていません
    echo.
    echo 以下のURLからPython 3.8以上をインストールしてください:
    echo https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

python --version
echo ✓ Python OK
echo.

REM 必要なライブラリをインストール
echo [2/3] 必要なライブラリをインストール中...
echo.
pip install --upgrade pip
pip install pandas openpyxl

if %errorlevel% neq 0 (
    echo.
    echo ❌ エラー: ライブラリのインストールに失敗しました
    echo.
    pause
    exit /b 1
)

echo.
echo ✓ ライブラリのインストール完了
echo.

REM データベースフォルダ作成
echo [3/3] データベースフォルダを作成中...
if not exist "%~dp0..\database" mkdir "%~dp0..\database"
echo ✓ フォルダ作成完了
echo.

REM 完了メッセージ
echo ========================================
echo   セットアップが完了しました！
echo ========================================
echo.
echo 次のステップ:
echo   1. データ更新: run_etl.bat を実行
echo   2. データ確認: DB Browser for SQLite で analytics.db を開く
echo.
pause
