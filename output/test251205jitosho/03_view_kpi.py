"""
==========================================
KPI閲覧スクリプト
KPI: 間接材調達コスト削減率
==========================================

このスクリプトは、構築済みのデータベースから
KPIデータを読み込んで表示します。

使用方法:
    python 03_view_kpi.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================
# 設定
# ============================================

BASE_DIR = Path(r"C:\Users\PC\dev\ForStep2")
DATABASE_PATH = BASE_DIR / "data" / "kpi_database.db"

# ============================================
# 表示関数
# ============================================

def print_section_header(title):
    """セクションヘッダーを表示"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def format_dataframe(df):
    """DataFrameを見やすくフォーマット"""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 30)
    pd.options.display.float_format = '{:,.2f}'.format
    return df

def view_overall_trend(conn):
    """全社削減率トレンド表示"""
    print_section_header("全社削減率トレンド（直近12ヶ月）")
    
    query = """
        SELECT 
            year_month AS 年月,
            current_amount AS 当月調達額,
            previous_year_amount AS 前年同月調達額,
            amount_difference AS 削減額,
            cost_reduction_rate AS コスト削減率,
            unit_price_reduction_rate AS 単価削減率
        FROM v_kpi_overall_trend
        ORDER BY year_month DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) == 0:
        print("データが存在しません")
        return
    
    df = format_dataframe(df)
    print(df.to_string(index=False))
    
    # サマリー統計
    print("\n【サマリー】")
    avg_reduction = df['コスト削減率'].mean()
    max_reduction = df['コスト削減率'].max()
    min_reduction = df['コスト削減率'].min()
    print(f"  平均削減率: {avg_reduction:.2f}%")
    print(f"  最大削減率: {max_reduction:.2f}% ({df[df['コスト削減率']==max_reduction]['年月'].values[0]})")
    print(f"  最小削減率: {min_reduction:.2f}% ({df[df['コスト削減率']==min_reduction]['年月'].values[0]})")

def view_supplier_ranking(conn):
    """サプライヤー別削減貢献ランキング表示"""
    print_section_header("サプライヤー別削減貢献ランキング（TOP10）")
    
    # 最新月を取得
    latest_month = pd.read_sql_query(
        "SELECT MAX(year_month) FROM gold_indirect_material_cost_reduction_rate",
        conn
    ).iloc[0, 0]
    
    print(f"対象年月: {latest_month}\n")
    
    query = f"""
        SELECT 
            axis_value AS サプライヤー名,
            current_amount AS 当月調達額,
            previous_year_amount AS 前年同月調達額,
            amount_difference AS 削減額,
            cost_reduction_rate AS 削減率
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'supplier'
          AND year_month = '{latest_month}'
          AND amount_difference > 0
        ORDER BY amount_difference DESC
        LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) == 0:
        print("データが存在しません")
        return
    
    df = format_dataframe(df)
    df.insert(0, '順位', range(1, len(df) + 1))
    print(df.to_string(index=False))
    
    # 合計
    total_saved = df['削減額'].sum()
    print(f"\nTOP10合計削減額: {total_saved:,.2f} 円")

def view_category_summary(conn):
    """カテゴリ別削減率サマリー表示"""
    print_section_header("資材カテゴリ別削減率サマリー")
    
    # 最新月を取得
    latest_month = pd.read_sql_query(
        "SELECT MAX(year_month) FROM gold_indirect_material_cost_reduction_rate",
        conn
    ).iloc[0, 0]
    
    print(f"対象年月: {latest_month}\n")
    
    query = f"""
        SELECT 
            axis_value AS 資材カテゴリ,
            current_amount AS 当月調達額,
            previous_year_amount AS 前年同月調達額,
            cost_reduction_rate AS コスト削減率,
            unit_price_reduction_rate AS 単価削減率
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'category'
          AND year_month = '{latest_month}'
        ORDER BY cost_reduction_rate DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) == 0:
        print("データが存在しません")
        return
    
    df = format_dataframe(df)
    print(df.to_string(index=False))

def view_location_summary(conn):
    """拠点別調達額サマリー表示"""
    print_section_header("拠点別調達額サマリー（直近月）")
    
    # 最新月を取得
    latest_month = pd.read_sql_query(
        "SELECT MAX(year_month) FROM gold_indirect_material_cost_monthly",
        conn
    ).iloc[0, 0]
    
    print(f"対象年月: {latest_month}\n")
    
    query = f"""
        SELECT 
            location_id AS 拠点,
            SUM(total_order_amount) AS 調達総額,
            SUM(order_count) AS 発注回数,
            COUNT(DISTINCT supplier_key) AS サプライヤー数,
            SUM(unique_material_count) AS 調達品目数
        FROM gold_indirect_material_cost_monthly
        WHERE year_month = '{latest_month}'
        GROUP BY location_id
        ORDER BY SUM(total_order_amount) DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) == 0:
        print("データが存在しません")
        return
    
    df = format_dataframe(df)
    print(df.to_string(index=False))
    
    # 合計
    total_amount = df['調達総額'].sum()
    total_orders = df['発注回数'].sum()
    print(f"\n全拠点合計: {total_amount:,.2f} 円 ({total_orders:,.0f} 発注)")

def view_data_quality(conn):
    """データ品質チェック結果表示"""
    print_section_header("データ品質チェック")
    
    # 異常な単価
    print("\n【異常な単価の検出】")
    query = "SELECT COUNT(*) as count FROM v_quality_check_abnormal_price"
    count = pd.read_sql_query(query, conn).iloc[0, 0]
    
    if count > 0:
        print(f"⚠ 異常な単価が {count} 件検出されました")
        query = """
            SELECT 
                purchase_order_id AS 発注ID,
                material_name AS 資材名,
                unit_price_ex_tax AS 単価,
                order_date AS 発注日
            FROM v_quality_check_abnormal_price
            LIMIT 5
        """
        df = pd.read_sql_query(query, conn)
        df = format_dataframe(df)
        print(df.to_string(index=False))
    else:
        print("✓ 異常な単価は検出されませんでした")
    
    # 異常な変動
    print("\n【異常な変動の検出（前年同月比±50%以上）】")
    query = "SELECT COUNT(*) as count FROM v_quality_check_abnormal_change"
    count = pd.read_sql_query(query, conn).iloc[0, 0]
    
    if count > 0:
        print(f"⚠ 異常な変動が {count} 件検出されました")
        query = """
            SELECT 
                year_month AS 年月,
                analysis_axis AS 分析軸,
                axis_value AS 対象,
                cost_reduction_rate AS 削減率
            FROM v_quality_check_abnormal_change
            LIMIT 5
        """
        df = pd.read_sql_query(query, conn)
        df = format_dataframe(df)
        print(df.to_string(index=False))
    else:
        print("✓ 異常な変動は検出されませんでした")
    
    # データ完全性
    print("\n【データ完全性（前年データの有無）】")
    query = "SELECT * FROM v_data_completeness_check LIMIT 5"
    df = pd.read_sql_query(query, conn)
    df.columns = ['年月', 'レコード数', '前年欠損数', '完全性率']
    df = format_dataframe(df)
    print(df.to_string(index=False))

def view_monthly_trend_chart(conn):
    """月次トレンドをテキストグラフで表示"""
    print_section_header("月次削減率トレンド（グラフ）")
    
    query = """
        SELECT 
            year_month,
            cost_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'overall'
        ORDER BY year_month
        LIMIT 12
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) == 0:
        print("データが存在しません")
        return
    
    print("\n月       削減率")
    print("-" * 50)
    
    for _, row in df.iterrows():
        month = row['year_month']
        rate = row['cost_reduction_rate']
        
        if pd.isna(rate):
            bar = "データなし"
        else:
            bar_length = int(abs(rate) / 2)  # スケール調整
            if rate >= 0:
                bar = "█" * bar_length + f" +{rate:.1f}%"
            else:
                bar = f" {rate:.1f}% " + "▓" * bar_length
        
        print(f"{month}  {bar}")

# ============================================
# メイン処理
# ============================================

def main():
    """メイン処理"""
    print("\n" + "=" * 70)
    print("  間接材調達コスト削減率 KPI レポート")
    print("  生成日時:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 70)
    
    # データベース接続確認
    if not DATABASE_PATH.exists():
        print(f"\nエラー: データベースファイルが見つかりません")
        print(f"パス: {DATABASE_PATH}")
        print("\n先に ETLスクリプト (02_etl_bronze_to_gold.py) を実行してください")
        return
    
    try:
        # データベース接続
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # KPIレポート表示
        view_overall_trend(conn)
        view_supplier_ranking(conn)
        view_category_summary(conn)
        view_location_summary(conn)
        view_monthly_trend_chart(conn)
        view_data_quality(conn)
        
        # 接続クローズ
        conn.close()
        
        print("\n" + "=" * 70)
        print("  レポート表示完了")
        print("=" * 70)
        print(f"\nデータベース: {DATABASE_PATH}")
        
    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        raise

if __name__ == "__main__":
    main()
