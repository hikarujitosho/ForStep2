"""
KPIダッシュボード用データエクスポートスクリプト
Excel/Power BIでの可視化用にCSVファイルを生成
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

DATABASE_PATH = Path(__file__).parent.parent / "database" / "analytics.db"
EXPORT_DIR = Path(__file__).parent.parent / "exports"

def create_export_directory():
    """エクスポートディレクトリを作成"""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp_dir = EXPORT_DIR / datetime.now().strftime('%Y%m%d_%H%M%S')
    timestamp_dir.mkdir(parents=True, exist_ok=True)
    return timestamp_dir

def export_kpi_trend(conn, export_dir):
    """全KPIの時系列トレンドをエクスポート"""
    print("KPIトレンドデータをエクスポート中...")
    
    # KPI1-5の月次トレンドを統合
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            'KPI1_在庫回転率' as kpi_name,
            ROUND(AVG(inventory_turnover_ratio), 2) as kpi_value,
            ROUND(AVG(achievement_rate), 1) as achievement_rate,
            12.0 as target_value
        FROM gold_kpi_inventory_turnover
        GROUP BY year_month
        
        UNION ALL
        
        SELECT
            year_month,
            'KPI2_リードタイム遵守率' as kpi_name,
            ROUND(AVG(lead_time_adherence_rate), 2) as kpi_value,
            ROUND(AVG(achievement_rate), 1) as achievement_rate,
            95.0 as target_value
        FROM gold_kpi_procurement_lead_time
        GROUP BY year_month
        
        UNION ALL
        
        SELECT
            year_month,
            'KPI3_物流コスト比率' as kpi_name,
            ROUND(AVG(logistics_cost_ratio), 2) as kpi_value,
            ROUND(AVG(achievement_rate), 1) as achievement_rate,
            5.0 as target_value
        FROM gold_kpi_logistics_cost_ratio
        GROUP BY year_month
        
        UNION ALL
        
        SELECT
            year_month,
            'KPI4_コスト削減率' as kpi_name,
            ROUND(AVG(cost_reduction_rate), 2) as kpi_value,
            ROUND(AVG(achievement_rate), 1) as achievement_rate,
            3.0 as target_value
        FROM gold_kpi_indirect_material_cost_reduction
        GROUP BY year_month
        
        UNION ALL
        
        SELECT
            year_month,
            'KPI5_CCC' as kpi_name,
            ROUND(AVG(cash_conversion_cycle), 1) as kpi_value,
            ROUND(AVG(achievement_rate), 1) as achievement_rate,
            60.0 as target_value
        FROM gold_kpi_cash_conversion_cycle
        GROUP BY year_month
        
        ORDER BY year_month, kpi_name
    """, conn)
    
    output_file = export_dir / "01_KPI_monthly_trend.csv"
    df_trend.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ {output_file.name} ({len(df_trend)}行)")
    
    return output_file

def export_location_comparison(conn, export_dir):
    """拠点別KPI比較をエクスポート"""
    print("拠点別比較データをエクスポート中...")
    
    # 最新月の拠点別KPI
    df_location = pd.read_sql("""
        WITH latest_month AS (
            SELECT MAX(year_month) as ym FROM gold_kpi_inventory_turnover
        )
        SELECT
            i.location_name as 拠点名,
            ROUND(i.inventory_turnover_ratio, 2) as 在庫回転率,
            i.evaluation as 在庫評価,
            ROUND(l.logistics_cost_ratio, 2) as 物流コスト比率,
            l.evaluation as 物流評価,
            ROUND(c.cash_conversion_cycle, 1) as CCC日数,
            c.evaluation as CCC評価,
            CASE
                WHEN i.evaluation = '優良' AND l.evaluation = '優良' AND c.evaluation = '優良' THEN 'A評価'
                WHEN (i.evaluation IN ('優良', '良好') AND l.evaluation IN ('優良', '良好') AND c.evaluation IN ('優良', '良好')) THEN 'B評価'
                ELSE 'C評価'
            END as 総合評価
        FROM gold_kpi_inventory_turnover i, latest_month
        JOIN gold_kpi_logistics_cost_ratio l ON i.location_key = l.location_key AND i.year_month = l.year_month
        JOIN gold_kpi_cash_conversion_cycle c ON i.location_key = c.location_key AND i.year_month = c.year_month
        WHERE i.year_month = latest_month.ym
          AND i.product_category = (SELECT product_category FROM gold_kpi_inventory_turnover WHERE year_month = latest_month.ym LIMIT 1)
        ORDER BY i.location_name
    """, conn)
    
    output_file = export_dir / "02_location_comparison.csv"
    df_location.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ {output_file.name} ({len(df_location)}行)")
    
    return output_file

def export_supplier_ranking(conn, export_dir):
    """サプライヤーランキングをエクスポート"""
    print("サプライヤーランキングをエクスポート中...")
    
    # リードタイムとコスト削減の統合評価
    df_supplier = pd.read_sql("""
        WITH latest_month AS (
            SELECT MAX(year_month) as ym FROM gold_kpi_procurement_lead_time
        ),
        lead_time AS (
            SELECT
                supplier_key,
                supplier_name,
                material_category,
                ROUND(lead_time_adherence_rate, 2) as リードタイム遵守率,
                evaluation as リードタイム評価,
                total_orders as 総注文数
            FROM gold_kpi_procurement_lead_time, latest_month
            WHERE year_month = ym
        ),
        cost_reduction AS (
            SELECT
                supplier_key,
                ROUND(cost_reduction_rate, 2) as コスト削減率,
                ROUND(total_savings / 1000000, 2) as 削減額_百万円,
                evaluation as コスト評価
            FROM gold_kpi_indirect_material_cost_reduction, latest_month
            WHERE year_month = ym
        )
        SELECT
            lt.supplier_name as サプライヤー名,
            lt.material_category as 材料カテゴリ,
            lt.総注文数,
            lt.リードタイム遵守率,
            lt.リードタイム評価,
            COALESCE(cr.コスト削減率, 0) as コスト削減率,
            COALESCE(cr.削減額_百万円, 0) as 削減額_百万円,
            COALESCE(cr.コスト評価, 'N/A') as コスト評価,
            CASE
                WHEN lt.リードタイム評価 = '優良' AND COALESCE(cr.コスト評価, 'N/A') = '優良' THEN 'S評価'
                WHEN lt.リードタイム評価 IN ('優良', '良好') THEN 'A評価'
                WHEN lt.リードタイム評価 = '要改善' THEN 'B評価'
                ELSE 'C評価'
            END as 総合評価
        FROM lead_time lt
        LEFT JOIN cost_reduction cr ON lt.supplier_key = cr.supplier_key
        WHERE lt.総注文数 >= 5
        ORDER BY lt.リードタイム遵守率 DESC, COALESCE(cr.コスト削減率, 0) DESC
    """, conn)
    
    output_file = export_dir / "03_supplier_ranking.csv"
    df_supplier.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ {output_file.name} ({len(df_supplier)}行)")
    
    return output_file

def export_action_items(conn, export_dir):
    """アクション項目リストをエクスポート"""
    print("アクション項目をエクスポート中...")
    
    df_actions = pd.read_sql("""
        SELECT
            'KPI1_在庫回転率' as KPI区分,
            location_name as 対象,
            ROUND(inventory_turnover_ratio, 2) as 現状値,
            ROUND(target_turnover, 2) as 目標値,
            evaluation as 評価,
            action_recommendation as アクションプラン,
            CASE
                WHEN evaluation = '要注意' THEN '高'
                WHEN evaluation = '要改善' THEN '中'
                ELSE '低'
            END as 優先度
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
          AND evaluation IN ('要注意', '要改善')
        
        UNION ALL
        
        SELECT
            'KPI2_リードタイム' as KPI区分,
            supplier_name || ' - ' || material_category as 対象,
            ROUND(lead_time_adherence_rate, 2) as 現状値,
            ROUND(target_adherence_rate, 2) as 目標値,
            evaluation as 評価,
            action_recommendation as アクションプラン,
            CASE
                WHEN evaluation = '要注意' THEN '高'
                WHEN evaluation = '要改善' THEN '中'
                ELSE '低'
            END as 優先度
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
          AND evaluation IN ('要注意', '要改善')
        
        UNION ALL
        
        SELECT
            'KPI3_物流コスト' as KPI区分,
            location_name as 対象,
            ROUND(logistics_cost_ratio, 2) as 現状値,
            ROUND(target_ratio, 2) as 目標値,
            evaluation as 評価,
            action_recommendation as アクションプラン,
            CASE
                WHEN evaluation = '要注意' THEN '高'
                WHEN evaluation = '要改善' THEN '中'
                ELSE '低'
            END as 優先度
        FROM gold_kpi_logistics_cost_ratio
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
          AND evaluation IN ('要注意', '要改善')
        
        ORDER BY 優先度 DESC, KPI区分
    """, conn)
    
    output_file = export_dir / "04_action_items.csv"
    df_actions.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ {output_file.name} ({len(df_actions)}行)")
    
    return output_file

def export_financial_impact(conn, export_dir):
    """財務インパクト試算をエクスポート"""
    print("財務インパクトをエクスポート中...")
    
    df_impact = pd.read_sql("""
        WITH latest_month AS (
            SELECT MAX(year_month) as ym FROM gold_kpi_inventory_turnover
        ),
        inventory_opportunity AS (
            SELECT
                SUM(avg_inventory_value) as current_inventory,
                SUM(CASE WHEN inventory_turnover_ratio < target_turnover
                    THEN avg_inventory_value * (target_turnover / inventory_turnover_ratio - 1)
                    ELSE 0 END) as reduction_opportunity
            FROM gold_kpi_inventory_turnover, latest_month
            WHERE year_month = ym
        ),
        logistics_opportunity AS (
            SELECT
                SUM(total_logistics_cost) as current_cost,
                SUM(CASE WHEN logistics_cost_ratio > target_ratio
                    THEN total_logistics_cost * (logistics_cost_ratio / target_ratio - 1)
                    ELSE 0 END) as reduction_opportunity
            FROM gold_kpi_logistics_cost_ratio, latest_month
            WHERE year_month = ym
        ),
        procurement_savings AS (
            SELECT
                SUM(total_savings) as realized_savings,
                SUM(CASE WHEN cost_reduction_rate < target_reduction_rate
                    THEN ABS(total_savings) * ((target_reduction_rate - cost_reduction_rate) / 100)
                    ELSE 0 END) as potential_savings
            FROM gold_kpi_indirect_material_cost_reduction, latest_month
            WHERE year_month = ym
        )
        SELECT
            '在庫削減機会' as 項目,
            ROUND(reduction_opportunity / 1000000, 2) as 金額_百万円,
            'KPI1改善による運転資本削減' as 説明
        FROM inventory_opportunity
        
        UNION ALL
        
        SELECT
            '物流コスト削減機会' as 項目,
            ROUND(reduction_opportunity / 1000000, 2) as 金額_百万円,
            'KPI3改善によるコスト削減' as 説明
        FROM logistics_opportunity
        
        UNION ALL
        
        SELECT
            '間接材コスト削減実績' as 項目,
            ROUND(realized_savings / 1000000, 2) as 金額_百万円,
            'KPI4既存実績' as 説明
        FROM procurement_savings
        
        UNION ALL
        
        SELECT
            '間接材追加削減機会' as 項目,
            ROUND(potential_savings / 1000000, 2) as 金額_百万円,
            'KPI4目標達成による追加削減' as 説明
        FROM procurement_savings
    """, conn)
    
    output_file = export_dir / "05_financial_impact.csv"
    df_impact.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ {output_file.name} ({len(df_impact)}行)")
    
    return output_file

def create_readme(export_dir):
    """エクスポートディレクトリのREADMEを作成"""
    readme_content = f"""# KPIダッシュボード用データエクスポート

**エクスポート日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}

## ファイル一覧

### 01_KPI_monthly_trend.csv
- **内容**: 全KPI（5つ）の月次推移データ
- **用途**: トレンドライングラフ、達成率推移の可視化
- **カラム**: year_month, kpi_name, kpi_value, achievement_rate, target_value

### 02_location_comparison.csv
- **内容**: 拠点別KPI比較（最新月）
- **用途**: 拠点別パフォーマンス評価、ヒートマップ
- **カラム**: 拠点名, 在庫回転率, 在庫評価, 物流コスト比率, 物流評価, CCC日数, CCC評価, 総合評価

### 03_supplier_ranking.csv
- **内容**: サプライヤー別パフォーマンスランキング（最新月）
- **用途**: サプライヤー評価、契約更新判断
- **カラム**: サプライヤー名, 材料カテゴリ, 総注文数, リードタイム遵守率, リードタイム評価, コスト削減率, 削減額_百万円, コスト評価, 総合評価

### 04_action_items.csv
- **内容**: 改善が必要な項目のアクションリスト（最新月）
- **用途**: 月次レビュー資料、タスク管理
- **カラム**: KPI区分, 対象, 現状値, 目標値, 評価, アクションプラン, 優先度

### 05_financial_impact.csv
- **内容**: KPI改善による財務インパクト試算
- **用途**: 経営報告、投資判断
- **カラム**: 項目, 金額_百万円, 説明

## 使用方法

### Excel
1. Excelを開く
2. データタブ → テキストまたはCSVから → ファイルを選択
3. 区切り記号: カンマ、文字エンコード: UTF-8
4. ピボットテーブルやグラフを作成

### Power BI
1. Power BI Desktop を開く
2. ホームタブ → データを取得 → テキスト/CSV
3. このフォルダ内のCSVファイルを選択
4. データ変換 → リレーションシップを設定
5. ビジュアライゼーションを作成

### Python (pandas)
```python
import pandas as pd

# トレンドデータ読み込み
df_trend = pd.read_csv('01_KPI_monthly_trend.csv')

# 拠点比較データ読み込み
df_location = pd.read_csv('02_location_comparison.csv')

# グラフ作成など
```

## 更新頻度
- **推奨**: 月次（月初5営業日以内）
- **方法**: `run_etl.bat` → `generate_kpi_report.py` → `export_dashboard_data.py`

## 注意事項
- CSVファイルはUTF-8エンコーディングです
- Excelで開く際は文字化けに注意（UTF-8対応版を使用）
- 数値は小数点第2位まで丸められています
"""
    
    readme_path = export_dir / "README.md"
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"  ✓ README.md")

def main():
    """メイン処理"""
    print(f"\n{'='*70}")
    print("  KPIダッシュボード用データエクスポート")
    print(f"{'='*70}\n")
    
    # エクスポートディレクトリ作成
    export_dir = create_export_directory()
    print(f"エクスポート先: {export_dir}\n")
    
    # データベース接続
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        # 各種データをエクスポート
        export_kpi_trend(conn, export_dir)
        export_location_comparison(conn, export_dir)
        export_supplier_ranking(conn, export_dir)
        export_action_items(conn, export_dir)
        export_financial_impact(conn, export_dir)
        
        # README作成
        create_readme(export_dir)
        
        print(f"\n{'='*70}")
        print("✓ データエクスポートが完了しました！")
        print(f"  保存先: {export_dir}")
        print(f"  ファイル数: 6個（CSV×5 + README）")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
