"""
==========================================
KPIモニタリングレポート生成スクリプト
KPI: 間接材調達コスト削減率
==========================================

このスクリプトは、以下の多角的な分析レポートを生成します:
1. KPI時系列推移分析
2. セグメント別KPI比較
3. サプライヤー別パフォーマンス分析
4. 資材カテゴリ別分析
5. 拠点別分析
6. 異常値・アラート検出
7. 予算達成状況分析

使用方法:
    python 04_generate_kpi_report.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 日本語フォント設定（Windowsの場合）
try:
    plt.rcParams['font.family'] = 'MS Gothic'
except:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans']

# ============================================
# 設定
# ============================================

BASE_DIR = Path(r"C:\Users\PC\dev\ForStep2")
DATABASE_PATH = BASE_DIR / "data" / "kpi_database.db"
REPORT_DIR = BASE_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# レポート出力パス
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
REPORT_HTML_PATH = REPORT_DIR / f"kpi_report_{TIMESTAMP}.html"
REPORT_EXCEL_PATH = REPORT_DIR / f"kpi_report_{TIMESTAMP}.xlsx"

# ============================================
# データ取得関数
# ============================================

def get_overall_trend(conn):
    """全社レベルの時系列トレンドデータ取得"""
    query = """
        SELECT 
            year_month,
            current_amount,
            previous_year_amount,
            amount_difference,
            cost_reduction_rate,
            current_avg_unit_price,
            previous_year_avg_unit_price,
            unit_price_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'overall'
        ORDER BY year_month
    """
    return pd.read_sql_query(query, conn)

def get_supplier_analysis(conn):
    """サプライヤー別分析データ取得"""
    query = """
        SELECT 
            year_month,
            axis_value AS supplier_name,
            axis_key AS supplier_key,
            current_amount,
            previous_year_amount,
            amount_difference,
            cost_reduction_rate,
            unit_price_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'supplier'
        ORDER BY year_month DESC, amount_difference DESC
    """
    return pd.read_sql_query(query, conn)

def get_category_analysis(conn):
    """資材カテゴリ別分析データ取得"""
    query = """
        SELECT 
            year_month,
            axis_value AS material_category,
            current_amount,
            previous_year_amount,
            amount_difference,
            cost_reduction_rate,
            unit_price_reduction_rate
        FROM gold_indirect_material_cost_reduction_rate
        WHERE analysis_axis = 'category'
        ORDER BY year_month DESC, cost_reduction_rate DESC
    """
    return pd.read_sql_query(query, conn)

def get_location_analysis(conn):
    """拠点別分析データ取得"""
    query = """
        SELECT 
            year_month,
            location_id,
            SUM(total_order_amount) AS total_amount,
            SUM(order_count) AS order_count,
            COUNT(DISTINCT supplier_key) AS supplier_count,
            SUM(unique_material_count) AS material_count,
            AVG(avg_unit_price) AS avg_price
        FROM gold_indirect_material_cost_monthly
        GROUP BY year_month, location_id
        ORDER BY year_month DESC, total_amount DESC
    """
    return pd.read_sql_query(query, conn)

def get_monthly_detail(conn):
    """月次詳細データ取得"""
    query = """
        SELECT 
            year_month,
            supplier_key,
            supplier_name,
            material_category,
            location_id,
            cost_center,
            total_order_amount,
            total_quantity,
            order_count,
            avg_unit_price,
            unique_material_count
        FROM gold_indirect_material_cost_monthly
        ORDER BY year_month DESC, total_order_amount DESC
    """
    return pd.read_sql_query(query, conn)

def get_quality_issues(conn):
    """データ品質問題取得"""
    abnormal_price = pd.read_sql_query(
        "SELECT * FROM v_quality_check_abnormal_price",
        conn
    )
    abnormal_change = pd.read_sql_query(
        "SELECT * FROM v_quality_check_abnormal_change",
        conn
    )
    return abnormal_price, abnormal_change

# ============================================
# 分析関数
# ============================================

def analyze_trend(df_trend):
    """トレンド分析"""
    analysis = {}
    
    # 基本統計
    df_valid = df_trend[df_trend['cost_reduction_rate'].notna()]
    if len(df_valid) > 0:
        analysis['avg_reduction_rate'] = df_valid['cost_reduction_rate'].mean()
        analysis['max_reduction_rate'] = df_valid['cost_reduction_rate'].max()
        analysis['min_reduction_rate'] = df_valid['cost_reduction_rate'].min()
        analysis['latest_reduction_rate'] = df_valid.iloc[-1]['cost_reduction_rate']
        analysis['total_cost_saved'] = df_valid['amount_difference'].sum()
    
    # トレンド判定（直近3ヶ月）
    if len(df_valid) >= 3:
        recent_3 = df_valid.tail(3)['cost_reduction_rate'].mean()
        previous_3 = df_valid.iloc[-6:-3]['cost_reduction_rate'].mean() if len(df_valid) >= 6 else None
        
        if previous_3 is not None:
            if recent_3 > previous_3:
                analysis['trend'] = '改善傾向'
            elif recent_3 < previous_3:
                analysis['trend'] = '悪化傾向'
            else:
                analysis['trend'] = '横ばい'
        else:
            analysis['trend'] = 'データ不足'
    else:
        analysis['trend'] = 'データ不足'
    
    return analysis

def analyze_suppliers(df_supplier):
    """サプライヤー分析"""
    analysis = {}
    
    # 最新月のデータ
    latest_month = df_supplier['year_month'].max()
    df_latest = df_supplier[df_supplier['year_month'] == latest_month]
    
    # TOP/BOTTOMサプライヤー
    if len(df_latest) > 0:
        analysis['top_performers'] = df_latest.nlargest(5, 'amount_difference')[
            ['supplier_name', 'amount_difference', 'cost_reduction_rate']
        ].to_dict('records')
        
        analysis['bottom_performers'] = df_latest.nsmallest(5, 'cost_reduction_rate')[
            ['supplier_name', 'amount_difference', 'cost_reduction_rate']
        ].to_dict('records')
    
    # サプライヤー数
    analysis['total_suppliers'] = df_supplier['supplier_key'].nunique()
    
    # コスト増加しているサプライヤー
    cost_increase = df_latest[df_latest['cost_reduction_rate'] < 0]
    analysis['cost_increase_suppliers'] = len(cost_increase)
    
    return analysis

def analyze_categories(df_category):
    """カテゴリ分析"""
    analysis = {}
    
    # 最新月のデータ
    latest_month = df_category['year_month'].max()
    df_latest = df_category[df_category['year_month'] == latest_month]
    
    if len(df_latest) > 0:
        analysis['best_category'] = df_latest.nlargest(1, 'cost_reduction_rate').iloc[0].to_dict()
        analysis['worst_category'] = df_latest.nsmallest(1, 'cost_reduction_rate').iloc[0].to_dict()
        analysis['category_performance'] = df_latest[
            ['material_category', 'current_amount', 'cost_reduction_rate']
        ].to_dict('records')
    
    return analysis

def analyze_locations(df_location):
    """拠点分析"""
    analysis = {}
    
    # 最新月のデータ
    latest_month = df_location['year_month'].max()
    df_latest = df_location[df_location['year_month'] == latest_month]
    
    if len(df_latest) > 0:
        analysis['total_locations'] = len(df_latest)
        analysis['top_locations'] = df_latest.nlargest(5, 'total_amount')[
            ['location_id', 'total_amount', 'supplier_count', 'order_count']
        ].to_dict('records')
        
        # 拠点別集中度
        total_amount = df_latest['total_amount'].sum()
        df_latest['share'] = df_latest['total_amount'] / total_amount * 100
        analysis['concentration'] = df_latest.nlargest(3, 'share')['share'].sum()
    
    return analysis

# ============================================
# HTMLレポート生成
# ============================================

def generate_html_report(conn, analyses):
    """HTMLレポート生成"""
    
    html = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>間接材調達コスト削減率 KPIモニタリングレポート</title>
        <style>
            body {{{{
                font-family: 'Segoe UI', 'MS Gothic', sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}}}
            .container {{{{
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }}}}
            h1 {{{{
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }}}}
            h2 {{{{
                color: #34495e;
                margin-top: 30px;
                border-left: 5px solid #3498db;
                padding-left: 10px;
            }}}}
            h3 {{{{
                color: #555;
                margin-top: 20px;
            }}}}
            .summary-box {{{{
                background-color: #ecf0f1;
                padding: 20px;
                border-radius: 5px;
                margin: 20px 0;
            }}}}
            .metric {{{{
                display: inline-block;
                margin: 10px 20px 10px 0;
            }}}}
            .metric-label {{{{
                font-size: 14px;
                color: #7f8c8d;
            }}}}
            .metric-value {{{{
                font-size: 24px;
                font-weight: bold;
                color: #2c3e50;
            }}}}
            .metric-value.positive {{{{
                color: #27ae60;
            }}}}
            .metric-value.negative {{{{
                color: #e74c3c;
            }}}}
            table {{{{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }}}}
            th {{{{
                background-color: #3498db;
                color: white;
                padding: 12px;
                text-align: left;
            }}}}
            td {{{{
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}}}
            tr:hover {{{{
                background-color: #f5f5f5;
            }}}}
            .alert {{{{
                background-color: #ffe5e5;
                border-left: 4px solid #e74c3c;
                padding: 15px;
                margin: 20px 0;
            }}}}
            .info {{{{
                background-color: #e5f2ff;
                border-left: 4px solid #3498db;
                padding: 15px;
                margin: 20px 0;
            }}}}
            .success {{{{
                background-color: #e5ffe5;
                border-left: 4px solid #27ae60;
                padding: 15px;
                margin: 20px 0;
            }}}}
            .footer {{{{
                margin-top: 50px;
                padding-top: 20px;
                border-top: 1px solid #ddd;
                text-align: center;
                color: #7f8c8d;
            }}}}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 間接材調達コスト削減率 KPIモニタリングレポート</h1>
            <p><strong>レポート生成日時:</strong> {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
            <p><strong>データベース:</strong> {DATABASE_PATH}</p>
    """
    
    # ==================== エグゼクティブサマリー ====================
    trend_analysis = analyses['trend']
    html += f"""
            <h2>🎯 エグゼクティブサマリー</h2>
            <div class="summary-box">
                <div class="metric">
                    <div class="metric-label">平均削減率</div>
                    <div class="metric-value {'positive' if trend_analysis.get('avg_reduction_rate', 0) > 0 else 'negative'}">
                        {trend_analysis.get('avg_reduction_rate', 0):.2f}%
                    </div>
                </div>
                <div class="metric">
                    <div class="metric-label">直近月削減率</div>
                    <div class="metric-value {'positive' if trend_analysis.get('latest_reduction_rate', 0) > 0 else 'negative'}">
                        {trend_analysis.get('latest_reduction_rate', 0):.2f}%
                    </div>
                </div>
                <div class="metric">
                    <div class="metric-label">累計削減額</div>
                    <div class="metric-value positive">
                        {trend_analysis.get('total_cost_saved', 0):,.0f} 円
                    </div>
                </div>
                <div class="metric">
                    <div class="metric-label">トレンド</div>
                    <div class="metric-value">
                        {trend_analysis.get('trend', 'データ不足')}
                    </div>
                </div>
            </div>
    """
    
    # トレンド判定メッセージ
    if trend_analysis.get('trend') == '改善傾向':
        html += '<div class="success">✅ 削減率は改善傾向にあります。引き続き施策を推進してください。</div>'
    elif trend_analysis.get('trend') == '悪化傾向':
        html += '<div class="alert">⚠️ 削減率が悪化傾向にあります。原因分析と対策が必要です。</div>'
    
    # ==================== KPI時系列推移 ====================
    df_trend = get_overall_trend(conn)
    html += f"""
            <h2>📈 KPI時系列推移（全社レベル）</h2>
            <h3>削減率・削減額の推移</h3>
            <table>
                <thead>
                    <tr>
                        <th>年月</th>
                        <th>当月調達額</th>
                        <th>前年同月調達額</th>
                        <th>削減額</th>
                        <th>コスト削減率</th>
                        <th>単価削減率</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for _, row in df_trend.tail(12).iterrows():
        reduction_class = 'positive' if pd.notna(row['cost_reduction_rate']) and row['cost_reduction_rate'] > 0 else 'negative'
        html += f"""
                    <tr>
                        <td>{row['year_month']}</td>
                        <td>{row['current_amount']:,.0f}</td>
                        <td>{row['previous_year_amount']:,.0f if pd.notna(row['previous_year_amount']) else '-'}</td>
                        <td>{row['amount_difference']:,.0f if pd.notna(row['amount_difference']) else '-'}</td>
                        <td class="{reduction_class}">{row['cost_reduction_rate']:.2f}% if pd.notna(row['cost_reduction_rate']) else '-'}</td>
                        <td>{row['unit_price_reduction_rate']:.2f}% if pd.notna(row['unit_price_reduction_rate']) else '-'}</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
    """
    
    # ==================== サプライヤー別分析 ====================
    supplier_analysis = analyses['supplier']
    html += f"""
            <h2>🏢 サプライヤー別パフォーマンス分析</h2>
            <div class="info">
                <strong>登録サプライヤー数:</strong> {supplier_analysis['total_suppliers']} 社<br>
                <strong>コスト増加サプライヤー数:</strong> {supplier_analysis['cost_increase_suppliers']} 社
            </div>
            
            <h3>TOP5 削減貢献サプライヤー</h3>
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
    
    for i, supplier in enumerate(supplier_analysis.get('top_performers', []), 1):
        html += f"""
                    <tr>
                        <td>{i}</td>
                        <td>{supplier['supplier_name']}</td>
                        <td>{supplier['amount_difference']:,.0f}</td>
                        <td class="positive">{supplier['cost_reduction_rate']:.2f}%</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
            
            <h3>要改善サプライヤー（削減率下位5社）</h3>
            <table>
                <thead>
                    <tr>
                        <th>サプライヤー名</th>
                        <th>削減額</th>
                        <th>削減率</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for supplier in supplier_analysis.get('bottom_performers', []):
        reduction_class = 'positive' if supplier['cost_reduction_rate'] > 0 else 'negative'
        html += f"""
                    <tr>
                        <td>{supplier['supplier_name']}</td>
                        <td>{supplier['amount_difference']:,.0f}</td>
                        <td class="{reduction_class}">{supplier['cost_reduction_rate']:.2f}%</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
    """
    
    # ==================== 資材カテゴリ別分析 ====================
    category_analysis = analyses['category']
    html += f"""
            <h2>📦 資材カテゴリ別分析</h2>
            <div class="summary-box">
                <strong>最優秀カテゴリ:</strong> {category_analysis.get('best_category', {}).get('material_category', '-')} 
                (削減率: {category_analysis.get('best_category', {}).get('cost_reduction_rate', 0):.2f}%)<br>
                <strong>要改善カテゴリ:</strong> {category_analysis.get('worst_category', {}).get('material_category', '-')}
                (削減率: {category_analysis.get('worst_category', {}).get('cost_reduction_rate', 0):.2f}%)
            </div>
            
            <h3>カテゴリ別パフォーマンス</h3>
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
    
    for cat in category_analysis.get('category_performance', []):
        reduction_class = 'positive' if cat['cost_reduction_rate'] > 0 else 'negative'
        html += f"""
                    <tr>
                        <td>{cat['material_category']}</td>
                        <td>{cat['current_amount']:,.0f}</td>
                        <td class="{reduction_class}">{cat['cost_reduction_rate']:.2f}%</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
    """
    
    # ==================== 拠点別分析 ====================
    location_analysis = analyses['location']
    html += f"""
            <h2>🏭 拠点別分析</h2>
            <div class="info">
                <strong>稼働拠点数:</strong> {location_analysis['total_locations']} 拠点<br>
                <strong>上位3拠点集中度:</strong> {location_analysis.get('concentration', 0):.1f}%
            </div>
            
            <h3>調達額TOP5拠点</h3>
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
    
    for i, loc in enumerate(location_analysis.get('top_locations', []), 1):
        html += f"""
                    <tr>
                        <td>{i}</td>
                        <td>{loc['location_id']}</td>
                        <td>{loc['total_amount']:,.0f}</td>
                        <td>{loc['supplier_count']:.0f}</td>
                        <td>{loc['order_count']:.0f}</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
    """
    
    # ==================== データ品質チェック ====================
    abnormal_price, abnormal_change = get_quality_issues(conn)
    html += f"""
            <h2>🔍 データ品質チェック</h2>
    """
    
    if len(abnormal_price) > 0:
        html += f"""
            <div class="alert">
                <strong>⚠️ 異常な単価検出:</strong> {len(abnormal_price)} 件<br>
                確認が必要な調達データがあります。詳細はデータベースの v_quality_check_abnormal_price ビューを参照してください。
            </div>
        """
    else:
        html += '<div class="success">✅ 異常な単価は検出されませんでした。</div>'
    
    if len(abnormal_change) > 0:
        html += f"""
            <div class="alert">
                <strong>⚠️ 異常な変動検出:</strong> {len(abnormal_change)} 件<br>
                前年同月比で±50%以上の変動があります。以下の項目を確認してください。
            </div>
            <table>
                <thead>
                    <tr>
                        <th>年月</th>
                        <th>分析軸</th>
                        <th>対象</th>
                        <th>削減率</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in abnormal_change.head(10).iterrows():
            html += f"""
                    <tr>
                        <td>{row['year_month']}</td>
                        <td>{row['analysis_axis']}</td>
                        <td>{row['axis_value']}</td>
                        <td class="negative">{row['cost_reduction_rate']:.2f}%</td>
                    </tr>
            """
        html += """
                </tbody>
            </table>
        """
    else:
        html += '<div class="success">✅ 異常な変動は検出されませんでした。</div>'
    
    # ==================== アクションアイテム ====================
    html += """
            <h2>📋 推奨アクションアイテム</h2>
            <div class="info">
                <h3>短期（1ヶ月以内）</h3>
                <ul>
                    <li>要改善サプライヤーとの価格交渉実施</li>
                    <li>異常な単価・変動の原因調査</li>
                    <li>コスト増加カテゴリの発注プロセス見直し</li>
                </ul>
                
                <h3>中期（3ヶ月以内）</h3>
                <ul>
                    <li>サプライヤー集約による規模のメリット追求</li>
                    <li>低削減率カテゴリの代替品検討</li>
                    <li>拠点間での調達ベストプラクティス共有</li>
                </ul>
                
                <h3>長期（6ヶ月以上）</h3>
                <ul>
                    <li>長期契約による価格固定化検討</li>
                    <li>新規サプライヤー開拓による競争促進</li>
                    <li>調達プロセスのデジタル化・自動化</li>
                </ul>
            </div>
    """
    
    # ==================== フッター ====================
    html += f"""
            <div class="footer">
                <p>このレポートは自動生成されました。</p>
                <p>データベース: {DATABASE_PATH}</p>
                <p>生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

# ============================================
# Excelレポート生成
# ============================================

def generate_excel_report(conn, analyses):
    """Excelレポート生成"""
    
    with pd.ExcelWriter(REPORT_EXCEL_PATH, engine='openpyxl') as writer:
        
        # シート1: サマリー
        summary_data = {
            '指標': [
                '平均削減率',
                '直近月削減率',
                '最大削減率',
                '最小削減率',
                '累計削減額',
                'トレンド',
                '登録サプライヤー数',
                'コスト増加サプライヤー数',
                '稼働拠点数'
            ],
            '値': [
                f"{analyses['trend'].get('avg_reduction_rate', 0):.2f}%",
                f"{analyses['trend'].get('latest_reduction_rate', 0):.2f}%",
                f"{analyses['trend'].get('max_reduction_rate', 0):.2f}%",
                f"{analyses['trend'].get('min_reduction_rate', 0):.2f}%",
                f"{analyses['trend'].get('total_cost_saved', 0):,.0f} 円",
                analyses['trend'].get('trend', 'データ不足'),
                analyses['supplier']['total_suppliers'],
                analyses['supplier']['cost_increase_suppliers'],
                analyses['location']['total_locations']
            ]
        }
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name='サマリー', index=False)
        
        # シート2: 時系列推移
        df_trend = get_overall_trend(conn)
        df_trend.to_excel(writer, sheet_name='時系列推移', index=False)
        
        # シート3: サプライヤー別
        df_supplier = get_supplier_analysis(conn)
        latest_month = df_supplier['year_month'].max()
        df_supplier_latest = df_supplier[df_supplier['year_month'] == latest_month]
        df_supplier_latest.to_excel(writer, sheet_name='サプライヤー別', index=False)
        
        # シート4: カテゴリ別
        df_category = get_category_analysis(conn)
        df_category_latest = df_category[df_category['year_month'] == latest_month]
        df_category_latest.to_excel(writer, sheet_name='カテゴリ別', index=False)
        
        # シート5: 拠点別
        df_location = get_location_analysis(conn)
        df_location_latest = df_location[df_location['year_month'] == latest_month]
        df_location_latest.to_excel(writer, sheet_name='拠点別', index=False)
        
        # シート6: 月次詳細
        df_monthly = get_monthly_detail(conn)
        df_monthly.to_excel(writer, sheet_name='月次詳細', index=False)
        
        # シート7: 品質チェック
        abnormal_price, abnormal_change = get_quality_issues(conn)
        if len(abnormal_price) > 0:
            abnormal_price.to_excel(writer, sheet_name='異常単価', index=False)
        if len(abnormal_change) > 0:
            abnormal_change.to_excel(writer, sheet_name='異常変動', index=False)

# ============================================
# メイン処理
# ============================================

def main():
    """メイン処理"""
    print("=" * 70)
    print("  KPIモニタリングレポート生成")
    print("=" * 70)
    print()
    
    # データベース接続確認
    if not DATABASE_PATH.exists():
        print(f"エラー: データベースファイルが見つかりません")
        print(f"パス: {DATABASE_PATH}")
        return
    
    try:
        # データベース接続
        print("データベースに接続中...")
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # データ取得と分析
        print("データ分析中...")
        
        df_trend = get_overall_trend(conn)
        df_supplier = get_supplier_analysis(conn)
        df_category = get_category_analysis(conn)
        df_location = get_location_analysis(conn)
        
        analyses = {
            'trend': analyze_trend(df_trend),
            'supplier': analyze_suppliers(df_supplier),
            'category': analyze_categories(df_category),
            'location': analyze_locations(df_location)
        }
        
        # HTMLレポート生成
        print("HTMLレポート生成中...")
        html_content = generate_html_report(conn, analyses)
        
        with open(REPORT_HTML_PATH, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ HTMLレポート: {REPORT_HTML_PATH}")
        
        # Excelレポート生成
        print("Excelレポート生成中...")
        generate_excel_report(conn, analyses)
        print(f"✓ Excelレポート: {REPORT_EXCEL_PATH}")
        
        # 接続クローズ
        conn.close()
        
        # 完了メッセージ
        print()
        print("=" * 70)
        print("  レポート生成完了")
        print("=" * 70)
        print()
        print("生成されたファイル:")
        print(f"  1. HTMLレポート: {REPORT_HTML_PATH}")
        print(f"  2. Excelレポート: {REPORT_EXCEL_PATH}")
        print()
        print("HTMLレポートをブラウザで開いて内容を確認してください。")
        
        # HTMLを自動で開く（オプション）
        import webbrowser
        webbrowser.open(str(REPORT_HTML_PATH))
        
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        raise

if __name__ == "__main__":
    main()
