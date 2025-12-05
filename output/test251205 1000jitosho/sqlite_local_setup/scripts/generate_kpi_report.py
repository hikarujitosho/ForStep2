"""
KPIモニタリングレポート生成スクリプト
複数の視点からKPIを分析してMarkdownレポートを作成
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# データベースパス
DATABASE_PATH = Path(__file__).parent.parent / "database" / "analytics.db"
REPORT_PATH = Path(__file__).parent.parent / "reports" / f"KPI_monitoring_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

def create_report_directory():
    """レポートディレクトリを作成"""
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_database_summary(conn):
    """データベースの概要を取得"""
    summary = []
    summary.append("# データベース概要\n")
    
    # テーブル一覧と件数
    tables = pd.read_sql("""
        SELECT name, 
               (SELECT COUNT(*) FROM sqlite_master sm2 WHERE sm2.name = sm.name) as count
        FROM sqlite_master sm
        WHERE type='table' AND name LIKE 'gold_%'
        ORDER BY name
    """, conn)
    
    summary.append("## Goldレイヤーテーブル\n")
    for _, row in tables.iterrows():
        table_name = row['name']
        count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn).iloc[0]['cnt']
        summary.append(f"- **{table_name}**: {count:,}件\n")
    
    return ''.join(summary)

def analyze_kpi1_inventory_turnover(conn):
    """KPI1: 在庫回転率の分析"""
    report = []
    report.append("\n---\n\n# KPI1: 在庫回転率\n")
    report.append("**ROIC貢献**: 在庫削減→運転資本減少→ROICアップ\n\n")
    
    # 1. 時系列推移（月次）
    report.append("## 1.1 時系列推移（月次平均）\n\n")
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            ROUND(AVG(inventory_turnover_ratio), 2) as avg_ratio,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent_count,
            COUNT(CASE WHEN evaluation = '要注意' THEN 1 END) as warning_count,
            COUNT(*) as total_locations
        FROM gold_kpi_inventory_turnover
        GROUP BY year_month
        ORDER BY year_month
    """, conn)
    
    report.append("| 年月 | 在庫回転率 | 達成率(%) | 優良拠点 | 要注意拠点 | 総拠点数 |\n")
    report.append("|------|-----------|----------|---------|-----------|----------|\n")
    for _, row in df_trend.iterrows():
        report.append(f"| {row['year_month']} | {row['avg_ratio']:.2f} | {row['avg_achievement']:.1f}% | {row['excellent_count']} | {row['warning_count']} | {row['total_locations']} |\n")
    
    # トレンド判定
    if len(df_trend) >= 2:
        latest = df_trend.iloc[-1]['avg_ratio']
        previous = df_trend.iloc[-2]['avg_ratio']
        trend = "📈 改善" if latest > previous else "📉 悪化" if latest < previous else "➡️ 横ばい"
        report.append(f"\n**トレンド**: {trend} (前月比: {latest - previous:+.2f})\n")
    
    # 2. 拠点別比較（最新月）
    report.append("\n## 1.2 拠点別パフォーマンス（最新月）\n\n")
    df_location = pd.read_sql("""
        SELECT
            location_name,
            product_category,
            ROUND(inventory_turnover_ratio, 2) as ratio,
            ROUND(achievement_rate, 1) as achievement,
            evaluation,
            action_recommendation
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
        ORDER BY inventory_turnover_ratio DESC
        LIMIT 10
    """, conn)
    
    report.append("| 順位 | 拠点名 | 製品カテゴリ | 在庫回転率 | 達成率(%) | 評価 |\n")
    report.append("|------|--------|------------|-----------|----------|------|\n")
    for idx, row in df_location.iterrows():
        report.append(f"| {idx+1} | {row['location_name']} | {row['product_category']} | {row['ratio']:.2f} | {row['achievement']:.1f}% | {row['evaluation']} |\n")
    
    # 3. 要注意拠点の詳細
    report.append("\n## 1.3 要注意拠点とアクションプラン\n\n")
    df_warning = pd.read_sql("""
        SELECT
            location_name,
            product_category,
            ROUND(inventory_turnover_ratio, 2) as ratio,
            ROUND(target_turnover, 2) as target,
            action_recommendation
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
          AND evaluation IN ('要注意', '要改善')
        ORDER BY inventory_turnover_ratio ASC
        LIMIT 5
    """, conn)
    
    if len(df_warning) > 0:
        report.append("| 拠点名 | 製品カテゴリ | 現状値 | 目標値 | アクションプラン |\n")
        report.append("|--------|------------|--------|--------|------------------|\n")
        for _, row in df_warning.iterrows():
            report.append(f"| {row['location_name']} | {row['product_category']} | {row['ratio']:.2f} | {row['target']:.2f} | {row['action_recommendation']} |\n")
    else:
        report.append("✅ 要注意拠点はありません\n")
    
    # 4. 製品カテゴリ別分析
    report.append("\n## 1.4 製品カテゴリ別分析（最新月）\n\n")
    df_category = pd.read_sql("""
        SELECT
            product_category,
            COUNT(DISTINCT location_name) as location_count,
            ROUND(AVG(inventory_turnover_ratio), 2) as avg_ratio,
            ROUND(MIN(inventory_turnover_ratio), 2) as min_ratio,
            ROUND(MAX(inventory_turnover_ratio), 2) as max_ratio,
            ROUND(AVG(achievement_rate), 1) as avg_achievement
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
        GROUP BY product_category
        ORDER BY avg_ratio DESC
    """, conn)
    
    report.append("| 製品カテゴリ | 拠点数 | 平均回転率 | 最小値 | 最大値 | 平均達成率(%) |\n")
    report.append("|------------|--------|-----------|--------|--------|---------------|\n")
    for _, row in df_category.iterrows():
        report.append(f"| {row['product_category']} | {row['location_count']} | {row['avg_ratio']:.2f} | {row['min_ratio']:.2f} | {row['max_ratio']:.2f} | {row['avg_achievement']:.1f}% |\n")
    
    return ''.join(report)

def analyze_kpi2_procurement_lead_time(conn):
    """KPI2: 調達リードタイム遵守率の分析"""
    report = []
    report.append("\n---\n\n# KPI2: 調達リードタイム遵守率\n")
    report.append("**ROIC貢献**: リードタイム短縮→在庫削減→運転資本減少→ROICアップ\n\n")
    
    # 1. 時系列推移
    report.append("## 2.1 時系列推移（月次平均）\n\n")
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            ROUND(AVG(lead_time_adherence_rate), 2) as avg_rate,
            ROUND(AVG(avg_lead_time_days), 1) as avg_days,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            SUM(total_orders) as total_orders,
            SUM(on_time_deliveries) as on_time_total,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent_suppliers
        FROM gold_kpi_procurement_lead_time
        GROUP BY year_month
        ORDER BY year_month
    """, conn)
    
    report.append("| 年月 | 遵守率(%) | 平均リードタイム(日) | 総注文数 | 期限内納品 | 優良サプライヤー数 |\n")
    report.append("|------|----------|-------------------|---------|-----------|------------------|\n")
    for _, row in df_trend.iterrows():
        report.append(f"| {row['year_month']} | {row['avg_rate']:.2f}% | {row['avg_days']:.1f} | {row['total_orders']:,} | {row['on_time_total']:,} | {row['excellent_suppliers']} |\n")
    
    # 2. サプライヤー別ランキング（最新月）
    report.append("\n## 2.2 サプライヤー別ランキング TOP10（最新月）\n\n")
    df_supplier = pd.read_sql("""
        SELECT
            supplier_name,
            material_category,
            total_orders,
            on_time_deliveries,
            ROUND(lead_time_adherence_rate, 2) as rate,
            ROUND(avg_lead_time_days, 1) as avg_days,
            evaluation
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
          AND total_orders >= 10
        ORDER BY lead_time_adherence_rate DESC
        LIMIT 10
    """, conn)
    
    report.append("| 順位 | サプライヤー名 | 材料カテゴリ | 総注文数 | 期限内納品 | 遵守率(%) | 平均日数 | 評価 |\n")
    report.append("|------|--------------|------------|---------|-----------|----------|---------|------|\n")
    for idx, row in df_supplier.iterrows():
        report.append(f"| {idx+1} | {row['supplier_name']} | {row['material_category']} | {row['total_orders']} | {row['on_time_deliveries']} | {row['rate']:.2f}% | {row['avg_days']:.1f} | {row['evaluation']} |\n")
    
    # 3. 問題サプライヤー
    report.append("\n## 2.3 改善が必要なサプライヤー（最新月）\n\n")
    df_problem = pd.read_sql("""
        SELECT
            supplier_name,
            material_category,
            total_orders,
            late_deliveries,
            ROUND(lead_time_adherence_rate, 2) as rate,
            ROUND(avg_lead_time_variance_days, 1) as avg_delay,
            action_recommendation
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
          AND evaluation IN ('要注意', '要改善')
          AND total_orders >= 5
        ORDER BY lead_time_adherence_rate ASC
        LIMIT 5
    """, conn)
    
    if len(df_problem) > 0:
        report.append("| サプライヤー名 | 材料カテゴリ | 総注文数 | 遅延数 | 遵守率(%) | 平均遅延(日) | アクションプラン |\n")
        report.append("|--------------|------------|---------|--------|----------|-------------|------------------|\n")
        for _, row in df_problem.iterrows():
            report.append(f"| {row['supplier_name']} | {row['material_category']} | {row['total_orders']} | {row['late_deliveries']} | {row['rate']:.2f}% | {row['avg_delay']:.1f} | {row['action_recommendation']} |\n")
    else:
        report.append("✅ 問題サプライヤーはありません\n")
    
    # 4. 材料カテゴリ別分析
    report.append("\n## 2.4 材料カテゴリ別分析（最新月）\n\n")
    df_material = pd.read_sql("""
        SELECT
            material_category,
            COUNT(DISTINCT supplier_name) as supplier_count,
            SUM(total_orders) as total_orders,
            ROUND(AVG(lead_time_adherence_rate), 2) as avg_rate,
            ROUND(AVG(avg_lead_time_days), 1) as avg_days
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
        GROUP BY material_category
        ORDER BY avg_rate DESC
    """, conn)
    
    report.append("| 材料カテゴリ | サプライヤー数 | 総注文数 | 平均遵守率(%) | 平均リードタイム(日) |\n")
    report.append("|------------|--------------|---------|--------------|-------------------|\n")
    for _, row in df_material.iterrows():
        report.append(f"| {row['material_category']} | {row['supplier_count']} | {row['total_orders']:,} | {row['avg_rate']:.2f}% | {row['avg_days']:.1f} |\n")
    
    return ''.join(report)

def analyze_kpi3_logistics_cost_ratio(conn):
    """KPI3: 物流コスト売上高比率の分析"""
    report = []
    report.append("\n---\n\n# KPI3: 物流コスト売上高比率\n")
    report.append("**ROIC貢献**: 物流コスト削減→営業利益率改善→ROICアップ\n\n")
    
    # 1. 時系列推移
    report.append("## 3.1 時系列推移（月次）\n\n")
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            ROUND(AVG(logistics_cost_ratio), 2) as avg_ratio,
            ROUND(SUM(total_logistics_cost) / 1000000, 2) as total_cost_million,
            ROUND(SUM(total_sales) / 1000000, 2) as total_sales_million,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent_count
        FROM gold_kpi_logistics_cost_ratio
        GROUP BY year_month
        ORDER BY year_month
    """, conn)
    
    report.append("| 年月 | コスト比率(%) | 物流コスト(百万円) | 売上高(百万円) | 達成率(%) | 優良拠点数 |\n")
    report.append("|------|-------------|-----------------|--------------|----------|------------|\n")
    for _, row in df_trend.iterrows():
        report.append(f"| {row['year_month']} | {row['avg_ratio']:.2f}% | {row['total_cost_million']:.2f} | {row['total_sales_million']:.2f} | {row['avg_achievement']:.1f}% | {row['excellent_count']} |\n")
    
    # 2. 拠点別比較（最新月）
    report.append("\n## 3.2 拠点別パフォーマンス（最新月）\n\n")
    df_location = pd.read_sql("""
        SELECT
            location_name,
            ROUND(logistics_cost_ratio, 2) as ratio,
            ROUND(total_logistics_cost / 1000000, 2) as cost_million,
            ROUND(total_sales / 1000000, 2) as sales_million,
            ROUND(achievement_rate, 1) as achievement,
            evaluation
        FROM gold_kpi_logistics_cost_ratio
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
        ORDER BY logistics_cost_ratio ASC
    """, conn)
    
    report.append("| 順位 | 拠点名 | コスト比率(%) | 物流コスト(百万円) | 売上高(百万円) | 達成率(%) | 評価 |\n")
    report.append("|------|--------|-------------|-----------------|--------------|----------|------|\n")
    for idx, row in df_location.iterrows():
        report.append(f"| {idx+1} | {row['location_name']} | {row['ratio']:.2f}% | {row['cost_million']:.2f} | {row['sales_million']:.2f} | {row['achievement']:.1f}% | {row['evaluation']} |\n")
    
    # 3. コスト内訳分析（最新月）
    report.append("\n## 3.3 物流コスト内訳分析（最新月）\n\n")
    df_breakdown = pd.read_sql("""
        SELECT
            location_name,
            ROUND(inbound_cost / 1000000, 2) as inbound,
            ROUND(outbound_cost / 1000000, 2) as outbound,
            ROUND(warehouse_cost / 1000000, 2) as warehouse,
            ROUND(total_logistics_cost / 1000000, 2) as total,
            ROUND(inbound_cost / total_logistics_cost * 100, 1) as inbound_pct,
            ROUND(outbound_cost / total_logistics_cost * 100, 1) as outbound_pct,
            ROUND(warehouse_cost / total_logistics_cost * 100, 1) as warehouse_pct
        FROM gold_kpi_logistics_cost_ratio
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
        ORDER BY total DESC
    """, conn)
    
    report.append("| 拠点名 | 入庫(百万円) | 出庫(百万円) | 倉庫(百万円) | 合計(百万円) | 入庫(%) | 出庫(%) | 倉庫(%) |\n")
    report.append("|--------|------------|------------|------------|------------|---------|---------|---------|\n")
    for _, row in df_breakdown.iterrows():
        report.append(f"| {row['location_name']} | {row['inbound']:.2f} | {row['outbound']:.2f} | {row['warehouse']:.2f} | {row['total']:.2f} | {row['inbound_pct']:.1f}% | {row['outbound_pct']:.1f}% | {row['warehouse_pct']:.1f}% |\n")
    
    return ''.join(report)

def analyze_kpi4_cost_reduction(conn):
    """KPI4: 間接材調達コスト削減率の分析"""
    report = []
    report.append("\n---\n\n# KPI4: 間接材調達コスト削減率\n")
    report.append("**ROIC貢献**: 調達コスト削減→営業利益率改善→ROICアップ\n\n")
    
    # 1. 時系列推移
    report.append("## 4.1 時系列推移（月次）\n\n")
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            ROUND(AVG(cost_reduction_rate), 2) as avg_rate,
            ROUND(SUM(total_savings) / 1000000, 2) as savings_million,
            COUNT(CASE WHEN is_improving = 1 THEN 1 END) as improving_count,
            COUNT(*) as total_items
        FROM gold_kpi_indirect_material_cost_reduction
        GROUP BY year_month
        ORDER BY year_month
    """, conn)
    
    report.append("| 年月 | 平均削減率(%) | 削減額累計(百万円) | 改善案件数 | 総案件数 |\n")
    report.append("|------|-------------|-----------------|-----------|----------|\n")
    for _, row in df_trend.iterrows():
        report.append(f"| {row['year_month']} | {row['avg_rate']:.2f}% | {row['savings_million']:.2f} | {row['improving_count']} | {row['total_items']} |\n")
    
    # 2. サプライヤー別削減貢献度（最新月）
    report.append("\n## 4.2 サプライヤー別コスト削減貢献度 TOP10（最新月）\n\n")
    df_supplier = pd.read_sql("""
        SELECT
            supplier_name,
            material_category,
            ROUND(cost_reduction_rate, 2) as rate,
            ROUND(total_savings / 1000000, 2) as savings_million,
            ROUND(quantity_procured, 0) as quantity,
            evaluation
        FROM gold_kpi_indirect_material_cost_reduction
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
        ORDER BY total_savings DESC
        LIMIT 10
    """, conn)
    
    report.append("| 順位 | サプライヤー名 | 材料カテゴリ | 削減率(%) | 削減額(百万円) | 調達数量 | 評価 |\n")
    report.append("|------|--------------|------------|----------|--------------|---------|------|\n")
    for idx, row in df_supplier.iterrows():
        report.append(f"| {idx+1} | {row['supplier_name']} | {row['material_category']} | {row['rate']:.2f}% | {row['savings_million']:.2f} | {row['quantity']:,.0f} | {row['evaluation']} |\n")
    
    # 3. 材料カテゴリ別分析（最新月）
    report.append("\n## 4.3 材料カテゴリ別削減実績（最新月）\n\n")
    df_category = pd.read_sql("""
        SELECT
            material_category,
            COUNT(DISTINCT supplier_name) as supplier_count,
            ROUND(AVG(cost_reduction_rate), 2) as avg_rate,
            ROUND(SUM(total_savings) / 1000000, 2) as savings_million,
            ROUND(SUM(quantity_procured), 0) as total_quantity
        FROM gold_kpi_indirect_material_cost_reduction
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
        GROUP BY material_category
        ORDER BY savings_million DESC
    """, conn)
    
    report.append("| 材料カテゴリ | サプライヤー数 | 平均削減率(%) | 削減額(百万円) | 総調達数量 |\n")
    report.append("|------------|--------------|-------------|--------------|------------|\n")
    for _, row in df_category.iterrows():
        report.append(f"| {row['material_category']} | {row['supplier_count']} | {row['avg_rate']:.2f}% | {row['savings_million']:.2f} | {row['total_quantity']:,.0f} |\n")
    
    # 4. 要改善案件（最新月）
    report.append("\n## 4.4 コスト増加案件（要注意）\n\n")
    df_warning = pd.read_sql("""
        SELECT
            supplier_name,
            material_category,
            ROUND(cost_reduction_rate, 2) as rate,
            ROUND(baseline_unit_price, 0) as baseline_price,
            ROUND(current_unit_price, 0) as current_price,
            action_recommendation
        FROM gold_kpi_indirect_material_cost_reduction
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
          AND cost_reduction_rate < 0
        ORDER BY cost_reduction_rate ASC
        LIMIT 5
    """, conn)
    
    if len(df_warning) > 0:
        report.append("| サプライヤー名 | 材料カテゴリ | 削減率(%) | ベース単価 | 現在単価 | アクションプラン |\n")
        report.append("|--------------|------------|----------|-----------|---------|------------------|\n")
        for _, row in df_warning.iterrows():
            report.append(f"| {row['supplier_name']} | {row['material_category']} | {row['rate']:.2f}% | ¥{row['baseline_price']:,.0f} | ¥{row['current_price']:,.0f} | {row['action_recommendation']} |\n")
    else:
        report.append("✅ コスト増加案件はありません\n")
    
    return ''.join(report)

def analyze_kpi5_cash_conversion_cycle(conn):
    """KPI5: キャッシュコンバージョンサイクルの分析"""
    report = []
    report.append("\n---\n\n# KPI5: キャッシュコンバージョンサイクル (CCC)\n")
    report.append("**ROIC貢献**: CCC短縮→運転資本減少→ROICアップ\n\n")
    
    # 1. 時系列推移
    report.append("## 5.1 時系列推移（月次）\n\n")
    df_trend = pd.read_sql("""
        SELECT
            year_month,
            ROUND(AVG(cash_conversion_cycle), 1) as avg_ccc,
            ROUND(AVG(dio), 1) as avg_dio,
            ROUND(AVG(dso), 1) as avg_dso,
            ROUND(AVG(dpo), 1) as avg_dpo,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent_count
        FROM gold_kpi_cash_conversion_cycle
        GROUP BY year_month
        ORDER BY year_month
    """, conn)
    
    report.append("| 年月 | CCC(日) | DIO(日) | DSO(日) | DPO(日) | 達成率(%) | 優良拠点数 |\n")
    report.append("|------|---------|---------|---------|---------|----------|------------|\n")
    for _, row in df_trend.iterrows():
        report.append(f"| {row['year_month']} | {row['avg_ccc']:.1f} | {row['avg_dio']:.1f} | {row['avg_dso']:.1f} | {row['avg_dpo']:.1f} | {row['avg_achievement']:.1f}% | {row['excellent_count']} |\n")
    
    # CCCの構成要素説明
    report.append("\n**CCCの構成**:\n")
    report.append("- **DIO** (Days Inventory Outstanding): 在庫回転日数\n")
    report.append("- **DSO** (Days Sales Outstanding): 売掛金回収日数\n")
    report.append("- **DPO** (Days Payables Outstanding): 買掛金支払日数\n")
    report.append("- **CCC** = DIO + DSO - DPO\n\n")
    
    # 2. 拠点別比較（最新月）
    report.append("\n## 5.2 拠点別CCC分析（最新月）\n\n")
    df_location = pd.read_sql("""
        SELECT
            location_name,
            ROUND(cash_conversion_cycle, 1) as ccc,
            ROUND(dio, 1) as dio,
            ROUND(dso, 1) as dso,
            ROUND(dpo, 1) as dpo,
            ROUND(achievement_rate, 1) as achievement,
            evaluation
        FROM gold_kpi_cash_conversion_cycle
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
        ORDER BY cash_conversion_cycle ASC
    """, conn)
    
    report.append("| 順位 | 拠点名 | CCC(日) | DIO(日) | DSO(日) | DPO(日) | 達成率(%) | 評価 |\n")
    report.append("|------|--------|---------|---------|---------|---------|----------|------|\n")
    for idx, row in df_location.iterrows():
        report.append(f"| {idx+1} | {row['location_name']} | {row['ccc']:.1f} | {row['dio']:.1f} | {row['dso']:.1f} | {row['dpo']:.1f} | {row['achievement']:.1f}% | {row['evaluation']} |\n")
    
    # 3. CCC構成要素の詳細分析（最新月）
    report.append("\n## 5.3 CCC構成要素の詳細分析（最新月）\n\n")
    df_components = pd.read_sql("""
        SELECT
            location_name,
            ROUND(avg_inventory / 1000000, 2) as inventory_million,
            ROUND(avg_receivables / 1000000, 2) as receivables_million,
            ROUND(avg_payables / 1000000, 2) as payables_million,
            ROUND(dio, 1) as dio,
            ROUND(dso, 1) as dso,
            ROUND(dpo, 1) as dpo
        FROM gold_kpi_cash_conversion_cycle
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
        ORDER BY location_name
    """, conn)
    
    report.append("| 拠点名 | 平均在庫(百万円) | 平均売掛金(百万円) | 平均買掛金(百万円) | DIO | DSO | DPO |\n")
    report.append("|--------|----------------|-----------------|----------------|-----|-----|-----|\n")
    for _, row in df_components.iterrows():
        report.append(f"| {row['location_name']} | {row['inventory_million']:.2f} | {row['receivables_million']:.2f} | {row['payables_million']:.2f} | {row['dio']:.1f} | {row['dso']:.1f} | {row['dpo']:.1f} |\n")
    
    return ''.join(report)

def create_executive_summary(conn):
    """エグゼクティブサマリー作成"""
    report = []
    report.append("\n---\n\n# エグゼクティブサマリー\n\n")
    
    # 最新月の全KPI概要
    report.append("## 📊 最新月KPI概要\n\n")
    
    # KPI1
    kpi1 = pd.read_sql("""
        SELECT
            ROUND(AVG(inventory_turnover_ratio), 2) as avg_value,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
            COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
    """, conn).iloc[0]
    
    report.append(f"### KPI1: 在庫回転率\n")
    report.append(f"- **平均値**: {kpi1['avg_value']:.2f}回/年 (目標: 12.0)\n")
    report.append(f"- **達成率**: {kpi1['avg_achievement']:.1f}%\n")
    report.append(f"- **優良拠点**: {kpi1['excellent']}拠点 / **要改善**: {kpi1['warning']}拠点\n\n")
    
    # KPI2
    kpi2 = pd.read_sql("""
        SELECT
            ROUND(AVG(lead_time_adherence_rate), 2) as avg_value,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
            COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
    """, conn).iloc[0]
    
    report.append(f"### KPI2: 調達リードタイム遵守率\n")
    report.append(f"- **平均値**: {kpi2['avg_value']:.2f}% (目標: 95.0%)\n")
    report.append(f"- **達成率**: {kpi2['avg_achievement']:.1f}%\n")
    report.append(f"- **優良サプライヤー**: {kpi2['excellent']}社 / **要改善**: {kpi2['warning']}社\n\n")
    
    # KPI3
    kpi3 = pd.read_sql("""
        SELECT
            ROUND(AVG(logistics_cost_ratio), 2) as avg_value,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
            COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
        FROM gold_kpi_logistics_cost_ratio
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
    """, conn).iloc[0]
    
    report.append(f"### KPI3: 物流コスト売上高比率\n")
    report.append(f"- **平均値**: {kpi3['avg_value']:.2f}% (目標: 5.0%)\n")
    report.append(f"- **達成率**: {kpi3['avg_achievement']:.1f}%\n")
    report.append(f"- **優良拠点**: {kpi3['excellent']}拠点 / **要改善**: {kpi3['warning']}拠点\n\n")
    
    # KPI4
    kpi4 = pd.read_sql("""
        SELECT
            ROUND(AVG(cost_reduction_rate), 2) as avg_value,
            ROUND(SUM(total_savings) / 1000000, 2) as total_savings,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
            COUNT(CASE WHEN cost_reduction_rate < 0 THEN 1 END) as warning
        FROM gold_kpi_indirect_material_cost_reduction
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
    """, conn).iloc[0]
    
    report.append(f"### KPI4: 間接材調達コスト削減率\n")
    report.append(f"- **平均削減率**: {kpi4['avg_value']:.2f}% (目標: 3.0%)\n")
    report.append(f"- **削減額累計**: {kpi4['total_savings']:.2f}百万円\n")
    report.append(f"- **優良案件**: {kpi4['excellent']}件 / **コスト増加**: {kpi4['warning']}件\n\n")
    
    # KPI5
    kpi5 = pd.read_sql("""
        SELECT
            ROUND(AVG(cash_conversion_cycle), 1) as avg_value,
            ROUND(AVG(achievement_rate), 1) as avg_achievement,
            COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
            COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
        FROM gold_kpi_cash_conversion_cycle
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
    """, conn).iloc[0]
    
    report.append(f"### KPI5: キャッシュコンバージョンサイクル\n")
    report.append(f"- **平均CCC**: {kpi5['avg_value']:.1f}日 (目標: 60日)\n")
    report.append(f"- **達成率**: {kpi5['avg_achievement']:.1f}%\n")
    report.append(f"- **優良拠点**: {kpi5['excellent']}拠点 / **要改善**: {kpi5['warning']}拠点\n\n")
    
    # 総合評価
    report.append("\n## 🎯 重点アクション項目\n\n")
    
    # 最も改善が必要な項目を自動抽出
    actions = []
    
    if kpi1['avg_value'] < 12.0:
        actions.append(f"1. **在庫回転率の改善**: 現状{kpi1['avg_value']:.2f}回/年 → 目標12.0回/年（{kpi1['warning']}拠点で要改善）")
    
    if kpi2['avg_value'] < 95.0:
        actions.append(f"2. **調達リードタイム遵守率の向上**: 現状{kpi2['avg_value']:.2f}% → 目標95.0%（{kpi2['warning']}サプライヤーで要改善）")
    
    if kpi3['avg_value'] > 5.0:
        actions.append(f"3. **物流コストの削減**: 現状{kpi3['avg_value']:.2f}% → 目標5.0%（{kpi3['warning']}拠点で要改善）")
    
    if kpi4['avg_value'] < 3.0:
        actions.append(f"4. **間接材調達コスト削減の加速**: 現状{kpi4['avg_value']:.2f}% → 目標3.0%（{kpi4['warning']}件でコスト増加）")
    
    if kpi5['avg_value'] > 60.0:
        actions.append(f"5. **CCCの短縮**: 現状{kpi5['avg_value']:.1f}日 → 目標60日（{kpi5['warning']}拠点で要改善）")
    
    if actions:
        for action in actions:
            report.append(f"{action}\n\n")
    else:
        report.append("✅ 全KPIが目標を達成しています。現状維持と継続的改善を推進してください。\n\n")
    
    return ''.join(report)

def main():
    """メイン処理"""
    print(f"\n{'='*70}")
    print("  KPIモニタリングレポート生成")
    print(f"{'='*70}\n")
    
    # レポートディレクトリ作成
    create_report_directory()
    
    # データベース接続
    print(f"データベースに接続: {DATABASE_PATH}")
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        # レポート生成
        report_content = []
        
        # ヘッダー
        report_content.append(f"# KPIモニタリングレポート\n\n")
        report_content.append(f"**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        report_content.append(f"**データベース**: `{DATABASE_PATH.name}`\n\n")
        
        # データベース概要
        print("データベース概要を生成中...")
        report_content.append(get_database_summary(conn))
        
        # エグゼクティブサマリー
        print("エグゼクティブサマリーを生成中...")
        report_content.append(create_executive_summary(conn))
        
        # KPI1分析
        print("KPI1: 在庫回転率を分析中...")
        report_content.append(analyze_kpi1_inventory_turnover(conn))
        
        # KPI2分析
        print("KPI2: 調達リードタイム遵守率を分析中...")
        report_content.append(analyze_kpi2_procurement_lead_time(conn))
        
        # KPI3分析
        print("KPI3: 物流コスト売上高比率を分析中...")
        report_content.append(analyze_kpi3_logistics_cost_ratio(conn))
        
        # KPI4分析
        print("KPI4: 間接材調達コスト削減率を分析中...")
        report_content.append(analyze_kpi4_cost_reduction(conn))
        
        # KPI5分析
        print("KPI5: キャッシュコンバージョンサイクルを分析中...")
        report_content.append(analyze_kpi5_cash_conversion_cycle(conn))
        
        # レポート保存
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(''.join(report_content))
        
        print(f"\n{'='*70}")
        print("✓ KPIモニタリングレポートの生成が完了しました！")
        print(f"  保存先: {REPORT_PATH}")
        print(f"  サイズ: {REPORT_PATH.stat().st_size / 1024:.1f} KB")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
