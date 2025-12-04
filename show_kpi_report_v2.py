#!/usr/bin/env python3
"""
KPIレポート表示ツール（全12指標対応版）
ゴールド層データからKPIレポートを生成・表示
"""

import os
import sys
import sqlite3
import pandas as pd

# プロジェクトルートを追加
sys.path.append(os.path.dirname(__file__))
from etl.common.database import DatabaseManager

def show_kpi_report():
    """全12指標のKPIレポートを表示"""
    try:
        # データベース接続
        db_manager = DatabaseManager()
        
        print("📊 ===========================================")
        print("📊 KPIレポート（全12指標）")
        print("📊 ===========================================")
        
        # 1. 月次EV販売率トレンド
        print("\n🚗 月次EV販売率トレンド")
        print("--------------------------------------------------")
        try:
            ev_sales_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月', 
                       ROUND(total_revenue/1000000, 1) AS '総売上(百万円)',
                       ROUND(ev_revenue/1000000, 1) AS 'EV売上(百万円)',
                       ev_sales_share AS 'EV比率(%)'
                FROM gold_monthly_ev_sales_share 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not ev_sales_df.empty:
                print(ev_sales_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 2. 月次先進安全装置適用率
        print("\n🛡️  月次先進安全装置適用率")
        print("--------------------------------------------------")
        try:
            safety_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月', 
                       safety_equipment_adoption_rate AS '安全装置適用率(%)'
                FROM gold_monthly_safety_equipment_adoption 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not safety_df.empty:
                print(safety_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 3. 月次EBITDA推移
        print("\n💰 月次EBITDA推移")
        print("--------------------------------------------------")
        try:
            ebitda_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月',
                       ROUND(revenue/1000000, 1) AS '売上(百万円)',
                       ROUND(gross_profit/1000000, 2) AS '粗利(百万円)',
                       ROUND(ebitda/1000000, 2) AS 'EBITDA(百万円)',
                       ebitda_margin AS 'EBITDA率(%)'
                FROM gold_monthly_ebitda 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not ebitda_df.empty:
                print(ebitda_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 4. 緊急輸送費率推移
        print("\n🚛 緊急輸送費率推移")
        print("--------------------------------------------------")
        try:
            transport_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月', 
                       ROUND(total_cost/1000, 1) AS '総輸送費(千円)',
                       ROUND(emergency_cost/1000, 1) AS '緊急輸送費(千円)',
                       emergency_cost_share AS '緊急輸送率(%)'
                FROM gold_emergency_transportation_cost_share 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not transport_df.empty:
                print(transport_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 5. 拠点別EV販売率（直近月）
        print("\n🏭 拠点別EV販売率（直近月）")
        print("--------------------------------------------------")
        try:
            area_ev_df = db_manager.query_to_dataframe("""
                SELECT location_id AS '拠点ID', 
                       location_name AS '拠点名',
                       ev_sales_share AS 'EV比率(%)'
                FROM gold_monthly_area_ev_sales_share 
                WHERE year_month = (SELECT MAX(year_month) FROM gold_monthly_area_ev_sales_share)
                ORDER BY ev_sales_share DESC
            """)
            if not area_ev_df.empty:
                print(area_ev_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 6. 商品別粗利率ランキング（直近月）
        print("\n📈 商品別粗利率ランキング（直近月）")
        print("--------------------------------------------------")
        try:
            product_margin_df = db_manager.query_to_dataframe("""
                SELECT product_id AS '製品ID', 
                       product_name AS '製品名',
                       gross_margin AS '粗利率(%)',
                       ROUND(revenue/1000000, 1) AS '売上(百万円)'
                FROM gold_monthly_product_gross_margin 
                WHERE year_month = (SELECT MAX(year_month) FROM gold_monthly_product_gross_margin)
                ORDER BY gross_margin DESC
                LIMIT 10
            """)
            if not product_margin_df.empty:
                print(product_margin_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 7. 棚卸資産回転期間
        print("\n📦 棚卸資産回転期間推移")
        print("--------------------------------------------------")
        try:
            inventory_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月',
                       ROUND(avg_inventory_value/1000000, 1) AS '平均在庫額(百万円)',
                       rotation_period_days AS '回転期間(日)'
                FROM gold_inventory_rotation_period 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not inventory_df.empty:
                print(inventory_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 8. 商品別棚卸資産回転期間（直近月上位5商品）
        print("\n🔄 商品別棚卸資産回転期間（直近月・回転早い順）")
        print("--------------------------------------------------")
        try:
            product_rotation_df = db_manager.query_to_dataframe("""
                SELECT product_name AS '商品名',
                       ROUND(avg_inventory_value/1000000, 1) AS '平均在庫額(百万円)',
                       rotation_period_days AS '回転期間(日)'
                FROM gold_monthly_product_inventory_rotation 
                WHERE year_month = (SELECT MAX(year_month) FROM gold_monthly_product_inventory_rotation)
                AND rotation_period_days IS NOT NULL
                ORDER BY rotation_period_days ASC
                LIMIT 5
            """)
            if not product_rotation_df.empty:
                print(product_rotation_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 9. 商品別EBITDA（上位5商品）
        print("\n💎 商品別EBITDA（上位5商品・直近月）")
        print("--------------------------------------------------")
        try:
            product_ebitda_df = db_manager.query_to_dataframe("""
                SELECT product_name AS '商品名',
                       ROUND(revenue/1000000, 1) AS '売上(百万円)',
                       ebitda_margin AS 'EBITDA率(%)',
                       ROUND(ebitda/1000000, 2) AS 'EBITDA(百万円)'
                FROM gold_monthly_product_ebitda 
                WHERE year_month = (SELECT MAX(year_month) FROM gold_monthly_product_ebitda)
                ORDER BY ebitda_margin DESC
                LIMIT 5
            """)
            if not product_ebitda_df.empty:
                print(product_ebitda_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 10. 納期遵守率
        print("\n⏰ 納期遵守率推移")
        print("--------------------------------------------------")
        try:
            delivery_df = db_manager.query_to_dataframe("""
                SELECT year_month AS '年月',
                       total_orders AS '総注文数',
                       on_time_deliveries AS '期限内配送数',
                       compliance_rate AS '遵守率(%)'
                FROM gold_monthly_delivery_compliance_rate 
                ORDER BY year_month 
                LIMIT 10
            """)
            if not delivery_df.empty:
                print(delivery_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 11. 仕入先リードタイム遵守率（上位5社）
        print("\n🤝 仕入先リードタイム遵守率（上位5社・直近月）")
        print("--------------------------------------------------")
        try:
            supplier_df = db_manager.query_to_dataframe("""
                SELECT supplier_name AS '仕入先名',
                       total_orders AS '総注文数',
                       compliance_rate AS '遵守率(%)',
                       avg_lead_time_days AS '平均リードタイム(日)'
                FROM gold_monthly_supplier_lead_time_compliance 
                WHERE year_month = (SELECT MAX(year_month) FROM gold_monthly_supplier_lead_time_compliance)
                ORDER BY compliance_rate DESC
                LIMIT 5
            """)
            if not supplier_df.empty:
                print(supplier_df.to_string(index=False))
            else:
                print("データなし")
        except Exception as e:
            print(f"エラー: {e}")
        
        # 12. KPIサマリー
        print("\n📋 KPI計算状況サマリー")
        print("--------------------------------------------------")
        kpi_tables = [
            ("gold_monthly_ev_sales_share", "月次EV販売率"),
            ("gold_monthly_safety_equipment_adoption", "先進安全装置適用率"),
            ("gold_monthly_ebitda", "月次EBITDA"),
            ("gold_emergency_transportation_cost_share", "緊急輸送費率"),
            ("gold_monthly_area_ev_sales_share", "エリア別EV販売率"),
            ("gold_monthly_product_gross_margin", "商品別粗利率"),
            ("gold_inventory_rotation_period", "棚卸資産回転期間"),
            ("gold_monthly_product_inventory_rotation", "商品別棚卸資産回転期間"),
            ("gold_monthly_product_ebitda", "商品別EBITDA"),
            ("gold_monthly_delivery_compliance_rate", "納期遵守率"),
            ("gold_monthly_supplier_lead_time_compliance", "仕入先リードタイム遵守率"),
            ("gold_monthly_area_safety_equipment_adoption", "エリア別安全装置適用率")
        ]
        
        summary_data = []
        for table_name, kpi_name in kpi_tables:
            try:
                count = db_manager.query_to_dataframe(f"SELECT COUNT(*) as cnt FROM {table_name}").iloc[0]['cnt']
                summary_data.append({
                    'KPI名': kpi_name,
                    'データ件数': count,
                    'ステータス': '✅' if count > 0 else '❌'
                })
            except:
                summary_data.append({
                    'KPI名': kpi_name,
                    'データ件数': 0,
                    'ステータス': '❌'
                })
        
        summary_df = pd.DataFrame(summary_data)
        print(summary_df.to_string(index=False))
        
        calculated_count = len([item for item in summary_data if item['データ件数'] > 0])
        print(f"\n✅ 計算済みKPI: {calculated_count}/12")
        
        print("\n📊 ===========================================")
        print("📊 レポート表示完了（12指標）")
        print("📊 ===========================================")
        
    except Exception as e:
        print(f"❌ KPIレポート表示エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    show_kpi_report()