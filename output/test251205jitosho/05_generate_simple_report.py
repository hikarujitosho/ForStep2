"""
==========================================
KPIモニタリングレポート生成スクリプト (簡易版)
KPI: 間接材調達コスト削減率
==========================================
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import webbrowser

# ============================================
# 設定
# ============================================

BASE_DIR = Path(r"C:\Users\PC\dev\ForStep2")
DATABASE_PATH = BASE_DIR / "data" / "kpi_database.db"
REPORT_DIR = BASE_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
REPORT_HTML_PATH = REPORT_DIR / f"kpi_report_{TIMESTAMP}.html"
REPORT_EXCEL_PATH = REPORT_DIR / f"kpi_report_{TIMESTAMP}.xlsx"

# ============================================
# データ取得関数
# ============================================

def get_overall_trend(conn):
    """全社レベルの時系列トレンドデータ取得"""
    query = """
        SELECT *
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'overall'
        ORDER BY year_month
    """
    return pd.read_sql_query(query, conn)

def get_supplier_analysis(conn, latest_month):
    """サプライヤー別分析データ取得"""
    query = f"""
        SELECT 
            axis_value AS supplier_name,
            current_amount,
            previous_year_amount,
            amount_difference,
            cost_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'supplier'
          AND year_month = '{latest_month}'
        ORDER BY amount_difference DESC
    """
    return pd.read_sql_query(query, conn)

def get_category_analysis(conn, latest_month):
    """資材カテゴリ別分析データ取得"""
    query = f"""
        SELECT 
            axis_value AS material_category,
            current_amount,
            cost_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'category'
          AND year_month = '{latest_month}'
        ORDER BY cost_reduction_rate DESC
    """
    return pd.read_sql_query(query, conn)

def get_location_analysis(conn, latest_month):
    """拠点別分析データ取得"""
    query = f"""
        SELECT 
            location_id,
            SUM(total_order_amount) AS total_amount,
            SUM(order_count) AS order_count,
            COUNT(DISTINCT supplier_key) AS supplier_count
        FROM gold_indirect_material_cost_monthly
        WHERE year_month = '{latest_month}'
        GROUP BY location_id
        ORDER BY total_amount DESC
    """
    return pd.read_sql_query(query, conn)

# ============================================
# HTMLレポート生成
# ============================================

def generate_html_report(conn):
    """HTMLレポート生成"""
    
    # データ取得
    df_trend = get_overall_trend(conn)
    latest_month = df_trend['year_month'].max()
    df_supplier = get_supplier_analysis(conn, latest_month)
    df_category = get_category_analysis(conn, latest_month)
    df_location = get_location_analysis(conn, latest_month)
    
    # 基本統計
    df_valid = df_trend[df_trend['cost_reduction_rate'].notna()]
    avg_reduction = df_valid['cost_reduction_rate'].mean() if len(df_valid) > 0 else 0
    latest_reduction = df_valid.iloc[-1]['cost_reduction_rate'] if len(df_valid) > 0 else 0
    total_saved = df_valid['amount_difference'].sum() if len(df_valid) > 0 else 0
    
    # HTMLヘッダー
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>間接材調達コスト削減率 KPIレポート</title>
    <style>
        body { font-family: 'MS Gothic', sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; }
        h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; border-left: 5px solid #3498db; padding-left: 10px; }
        .summary-box { background: #ecf0f1; padding: 20px; border-radius: 5px; margin: 20px 0; }
        .metric { display: inline-block; margin: 10px 20px 10px 0; }
        .metric-label { font-size: 14px; color: #7f8c8d; }
        .metric-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
        .positive { color: #27ae60; }
        .negative { color: #e74c3c; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th { background: #3498db; color: white; padding: 12px; text-align: left; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        tr:hover { background: #f5f5f5; }
        .alert { background: #ffe5e5; border-left: 4px solid #e74c3c; padding: 15px; margin: 20px 0; }
        .info { background: #e5f2ff; border-left: 4px solid #3498db; padding: 15px; margin: 20px 0; }
        .success { background: #e5ffe5; border-left: 4px solid #27ae60; padding: 15px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 間接材調達コスト削減率 KPIモニタリングレポート</h1>
        <p><strong>レポート生成日時:</strong> """ + datetime.now().strftime('%Y年%m月%d日 %H:%M:%S') + """</p>
        <p><strong>対象年月:</strong> """ + latest_month + """</p>
        
        <h2>🎯 エグゼクティブサマリー</h2>
        <div class="summary-box">
            <div class="metric">
                <div class="metric-label">平均削減率</div>
                <div class="metric-value positive">""" + f"{avg_reduction:.2f}%" + """</div>
            </div>
            <div class="metric">
                <div class="metric-label">直近月削減率</div>
                <div class="metric-value positive">""" + f"{latest_reduction:.2f}%" + """</div>
            </div>
            <div class="metric">
                <div class="metric-label">累計削減額</div>
                <div class="metric-value positive">""" + f"{total_saved:,.0f}" + """ 円</div>
            </div>
        </div>
"""
    
    # 時系列推移
    html += """
        <h2>📈 KPI時系列推移（全社レベル）</h2>
        <table>
            <thead>
                <tr>
                    <th>年月</th>
                    <th>当月調達額</th>
                    <th>前年同月調達額</th>
                    <th>削減額</th>
                    <th>削減率</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for _, row in df_trend.tail(12).iterrows():
        prev_amt = f"{row['previous_year_amount']:,.0f}" if pd.notna(row['previous_year_amount']) else '-'
        amt_diff = f"{row['amount_difference']:,.0f}" if pd.notna(row['amount_difference']) else '-'
        reduction_rate = f"{row['cost_reduction_rate']:.2f}%" if pd.notna(row['cost_reduction_rate']) else '-'
        rate_class = 'positive' if pd.notna(row['cost_reduction_rate']) and row['cost_reduction_rate'] > 0 else 'negative'
        
        html += f"""
                <tr>
                    <td>{row['year_month']}</td>
                    <td>{row['current_amount']:,.0f}</td>
                    <td>{prev_amt}</td>
                    <td>{amt_diff}</td>
                    <td class="{rate_class}">{reduction_rate}</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    
    # サプライヤー別TOP5
    html += """
        <h2>🏢 サプライヤー別パフォーマンス（TOP5）</h2>
        <table>
            <thead>
                <tr>
                    <th>順位</th>
                    <th>サプライヤー名</th>
                    <th>削減額</th>
                    <th>削減率</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for i, (_, row) in enumerate(df_supplier.head(5).iterrows(), 1):
        html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{row['supplier_name']}</td>
                    <td>{row['amount_difference']:,.0f}</td>
                    <td class="positive">{row['cost_reduction_rate']:.2f}%</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    
    # カテゴリ別
    html += """
        <h2>📦 資材カテゴリ別パフォーマンス</h2>
        <table>
            <thead>
                <tr>
                    <th>資材カテゴリ</th>
                    <th>当月調達額</th>
                    <th>削減率</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for _, row in df_category.iterrows():
        rate_class = 'positive' if row['cost_reduction_rate'] > 0 else 'negative'
        html += f"""
                <tr>
                    <td>{row['material_category']}</td>
                    <td>{row['current_amount']:,.0f}</td>
                    <td class="{rate_class}">{row['cost_reduction_rate']:.2f}%</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    
    # 拠点別
    html += """
        <h2>🏭 拠点別調達額（TOP5）</h2>
        <table>
            <thead>
                <tr>
                    <th>順位</th>
                    <th>拠点</th>
                    <th>調達総額</th>
                    <th>サプライヤー数</th>
                    <th>発注回数</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for i, (_, row) in enumerate(df_location.head(5).iterrows(), 1):
        html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{row['location_id']}</td>
                    <td>{row['total_amount']:,.0f}</td>
                    <td>{row['supplier_count']:.0f}</td>
                    <td>{row['order_count']:.0f}</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
        
        <h2>📋 推奨アクション</h2>
        <div class="info">
            <h3>短期（1ヶ月以内）</h3>
            <ul>
                <li>削減率が低いサプライヤーとの価格交渉実施</li>
                <li>コスト増加カテゴリの発注プロセス見直し</li>
            </ul>
            
            <h3>中期（3ヶ月以内）</h3>
            <ul>
                <li>サプライヤー集約による規模のメリット追求</li>
                <li>低削減率カテゴリの代替品検討</li>
            </ul>
        </div>
        
        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #7f8c8d;">
            <p>このレポートは自動生成されました。</p>
            <p>データベース: """ + str(DATABASE_PATH) + """</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html

# ============================================
# Excelレポート生成
# ============================================

def generate_excel_report(conn):
    """Excelレポート生成"""
    
    df_trend = get_overall_trend(conn)
    latest_month = df_trend['year_month'].max()
    
    with pd.ExcelWriter(REPORT_EXCEL_PATH, engine='openpyxl') as writer:
        # 時系列推移
        df_trend.to_excel(writer, sheet_name='時系列推移', index=False)
        
        # サプライヤー別
        df_supplier = get_supplier_analysis(conn, latest_month)
        df_supplier.to_excel(writer, sheet_name='サプライヤー別', index=False)
        
        # カテゴリ別
        df_category = get_category_analysis(conn, latest_month)
        df_category.to_excel(writer, sheet_name='カテゴリ別', index=False)
        
        # 拠点別
        df_location = get_location_analysis(conn, latest_month)
        df_location.to_excel(writer, sheet_name='拠点別', index=False)

# ============================================
# メイン処理
# ============================================

def main():
    """メイン処理"""
    print("=" * 70)
    print("  KPIモニタリングレポート生成")
    print("=" * 70)
    print()
    
    if not DATABASE_PATH.exists():
        print(f"エラー: データベースが見つかりません: {DATABASE_PATH}")
        return
    
    try:
        print("データベースに接続中...")
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        print("HTMLレポート生成中...")
        html_content = generate_html_report(conn)
        
        with open(REPORT_HTML_PATH, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ HTMLレポート: {REPORT_HTML_PATH}")
        
        try:
            print("Excelレポート生成中...")
            generate_excel_report(conn)
            print(f"✓ Excelレポート: {REPORT_EXCEL_PATH}")
        except Exception as excel_err:
            print(f"警告: Excelレポート生成をスキップしました（{str(excel_err)}）")
        
        conn.close()
        
        print()
        print("=" * 70)
        print("  レポート生成完了")
        print("=" * 70)
        print()
        print("生成されたファイル:")
        print(f"  1. HTMLレポート: {REPORT_HTML_PATH}")
        print(f"  2. Excelレポート: {REPORT_EXCEL_PATH}")
        print()
        
        # HTMLを自動で開く
        webbrowser.open(str(REPORT_HTML_PATH))
        print("HTMLレポートをブラウザで開きました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
