#!/usr/bin/env python3
"""
ETLマスタースクリプト
ブロンズ → シルバー → ゴールド の全工程を実行
"""

import sys
import time
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).parent.parent))

from etl.common import setup_logging
from etl.bronze.bronze_etl import BronzeETL
from etl.silver.silver_etl import SilverETL
from etl.gold.gold_etl import GoldETL

def main():
    """メイン実行関数"""
    logger = setup_logging("etl_master")
    
    try:
        logger.info("=== メダリオンアーキテクチャETL開始 ===")
        start_time = time.time()
        
        # ブロンズ層ETL実行
        logger.info("--- ブロンズ層ETL実行 ---")
        bronze_start = time.time()
        bronze_etl = BronzeETL()
        if not bronze_etl.run():
            logger.error("ブロンズ層ETL実行に失敗しました")
            return 1
        bronze_time = time.time() - bronze_start
        logger.info(f"ブロンズ層ETL完了 (実行時間: {bronze_time:.2f}秒)")
        
        # シルバー層ETL実行
        logger.info("--- シルバー層ETL実行 ---")
        silver_start = time.time()
        silver_etl = SilverETL()
        if not silver_etl.run():
            logger.error("シルバー層ETL実行に失敗しました")
            return 1
        silver_time = time.time() - silver_start
        logger.info(f"シルバー層ETL完了 (実行時間: {silver_time:.2f}秒)")
        
        # ゴールド層ETL実行
        logger.info("--- ゴールド層ETL実行 ---")
        gold_start = time.time()
        gold_etl = GoldETL()
        if not gold_etl.run():
            logger.error("ゴールド層ETL実行に失敗しました")
            return 1
        gold_time = time.time() - gold_start
        logger.info(f"ゴールド層ETL完了 (実行時間: {gold_time:.2f}秒)")
        
        # 総実行時間
        total_time = time.time() - start_time
        logger.info(f"=== ETL全工程完了 (総実行時間: {total_time:.2f}秒) ===")
        
        # サマリー情報
        logger.info(f"実行時間内訳:")
        logger.info(f"  ブロンズ層: {bronze_time:.2f}秒 ({bronze_time/total_time*100:.1f}%)")
        logger.info(f"  シルバー層: {silver_time:.2f}秒 ({silver_time/total_time*100:.1f}%)")
        logger.info(f"  ゴールド層: {gold_time:.2f}秒 ({gold_time/total_time*100:.1f}%)")
        
        print()
        print("✅ ETL処理が正常に完了しました")
        print(f"📊 データレイクが正常に構築されました (database/data_lake.db)")
        print(f"⏱️  総実行時間: {total_time:.2f}秒")
        print()
        print("📈 計算されたKPI:")
        print("  • 月次商品別粗利率")
        print("  • 月次EV販売率")
        print("  • 月次エリア別EV販売率")
        print("  • 月次先進安全装置適用率")
        print("  • 月次エリア別先進安全装置適用率")
        print("  • 棚卸資産回転期間")
        print("  • 月次EBITDA")
        print("  • 緊急輸送費率")
        
        return 0
        
    except Exception as e:
        logger.error(f"ETL実行中にエラーが発生しました: {e}")
        print(f"❌ ETL処理でエラーが発生しました: {e}")
        return 1

if __name__ == "__main__":
    exit(main())