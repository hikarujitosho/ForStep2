"""
KPIモニタリングレポート HTML帳票生成スクリプト
視覚的に見やすいHTMLレポートを作成
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

DATABASE_PATH = Path(__file__).parent.parent / "database" / "analytics.db"
REPORT_PATH = Path(__file__).parent.parent / "reports" / f"KPI_monitoring_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

def create_html_header():
    """HTMLヘッダーとスタイルを作成"""
    return """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPIモニタリングレポート</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', 'Yu Gothic', Meiryo, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header .subtitle {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .report-info {
            background: rgba(255,255,255,0.1);
            padding: 15px;
            margin-top: 20px;
            border-radius: 8px;
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
        }
        
        .report-info-item {
            text-align: center;
            padding: 10px;
        }
        
        .report-info-item .label {
            font-size: 0.9em;
            opacity: 0.8;
        }
        
        .report-info-item .value {
            font-size: 1.3em;
            font-weight: bold;
            margin-top: 5px;
        }
        
        .content {
            padding: 40px;
        }
        
        .section {
            margin-bottom: 50px;
        }
        
        .section-title {
            font-size: 2em;
            color: #667eea;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-bottom: 30px;
            display: flex;
            align-items: center;
        }
        
        .section-title .icon {
            font-size: 1.2em;
            margin-right: 15px;
        }
        
        .kpi-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .kpi-card {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .kpi-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.2);
        }
        
        .kpi-card.excellent {
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        }
        
        .kpi-card.good {
            background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
        }
        
        .kpi-card.warning {
            background: linear-gradient(135deg, #ffd89b 0%, #19547b 100%);
            color: white;
        }
        
        .kpi-card-title {
            font-size: 1.1em;
            font-weight: bold;
            margin-bottom: 15px;
            color: #333;
        }
        
        .kpi-card.warning .kpi-card-title {
            color: white;
        }
        
        .kpi-value {
            font-size: 3em;
            font-weight: bold;
            margin: 15px 0;
            color: #667eea;
        }
        
        .kpi-card.warning .kpi-value {
            color: white;
        }
        
        .kpi-target {
            font-size: 0.9em;
            color: #666;
            margin-bottom: 10px;
        }
        
        .kpi-card.warning .kpi-target {
            color: rgba(255,255,255,0.9);
        }
        
        .achievement-bar {
            width: 100%;
            height: 8px;
            background: rgba(0,0,0,0.1);
            border-radius: 4px;
            overflow: hidden;
            margin: 10px 0;
        }
        
        .achievement-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.5s ease;
        }
        
        .kpi-card.warning .achievement-fill {
            background: linear-gradient(90deg, #ff6b6b 0%, #ee5a6f 100%);
        }
        
        .kpi-status {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            margin-top: 10px;
        }
        
        .status-excellent {
            background: #4caf50;
            color: white;
        }
        
        .status-good {
            background: #8bc34a;
            color: white;
        }
        
        .status-improvement {
            background: #ff9800;
            color: white;
        }
        
        .status-warning {
            background: #f44336;
            color: white;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 15px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }
        
        td {
            padding: 12px 15px;
            border-bottom: 1px solid #f0f0f0;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
        }
        
        tbody tr:last-child td {
            border-bottom: none;
        }
        
        .rank {
            display: inline-block;
            width: 30px;
            height: 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 50%;
            text-align: center;
            line-height: 30px;
            font-weight: bold;
        }
        
        .rank.top3 {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }
        
        .trend-up {
            color: #4caf50;
            font-weight: bold;
        }
        
        .trend-down {
            color: #f44336;
            font-weight: bold;
        }
        
        .trend-neutral {
            color: #9e9e9e;
        }
        
        .chart-container {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        
        .action-item {
            background: white;
            border-left: 4px solid #667eea;
            padding: 15px 20px;
            margin: 15px 0;
            border-radius: 4px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .action-item.high-priority {
            border-left-color: #f44336;
            background: #ffebee;
        }
        
        .action-item-title {
            font-weight: bold;
            margin-bottom: 8px;
            color: #333;
        }
        
        .action-item-description {
            color: #666;
            line-height: 1.6;
        }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        
        .summary-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }
        
        .summary-card-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }
        
        .summary-card-label {
            color: #666;
            font-size: 0.9em;
        }
        
        .footer {
            background: #f8f9fa;
            padding: 30px;
            text-align: center;
            color: #666;
            border-top: 1px solid #e0e0e0;
        }
        
        @media print {
            body {
                background: white;
                padding: 0;
            }
            
            .container {
                box-shadow: none;
            }
            
            .kpi-card:hover {
                transform: none;
            }
        }
        
        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8em;
            }
            
            .kpi-cards {
                grid-template-columns: 1fr;
            }
            
            table {
                font-size: 0.85em;
            }
            
            th, td {
                padding: 8px;
            }
        }
    </style>
</head>
<body>
"""

def create_executive_summary_html(conn):
    """エグゼクティブサマリーのHTML生成"""
    html = []
    
    # 最新月取得
    latest_month = pd.read_sql("SELECT MAX(year_month) as ym FROM gold_kpi_inventory_turnover", conn).iloc[0]['ym']
    
    html.append(f"""
    <div class="header">
        <h1>📊 KPIモニタリングレポート</h1>
        <p class="subtitle">自動車製造業 物流・間接材購買部門 ROIC改善KPI分析</p>
        <div class="report-info">
            <div class="report-info-item">
                <div class="label">生成日時</div>
                <div class="value">{datetime.now().strftime('%Y年%m月%d日 %H:%M')}</div>
            </div>
            <div class="report-info-item">
                <div class="label">分析期間</div>
                <div class="value">{latest_month}</div>
            </div>
            <div class="report-info-item">
                <div class="label">KPI数</div>
                <div class="value">5指標</div>
            </div>
        </div>
    </div>
    
    <div class="content">
        <div class="section">
            <h2 class="section-title"><span class="icon">🎯</span>エグゼクティブサマリー</h2>
    """)
    
    # 各KPIのカード表示
    kpis = [
        {
            'query': """
                SELECT
                    ROUND(AVG(inventory_turnover_ratio), 2) as value,
                    ROUND(AVG(achievement_rate), 1) as achievement,
                    12.0 as target,
                    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
                    COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
                FROM gold_kpi_inventory_turnover
                WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
            """,
            'name': 'KPI1: 在庫回転率',
            'unit': '回/年',
            'icon': '📦'
        },
        {
            'query': """
                SELECT
                    ROUND(AVG(lead_time_adherence_rate), 2) as value,
                    ROUND(AVG(achievement_rate), 1) as achievement,
                    95.0 as target,
                    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
                    COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
                FROM gold_kpi_procurement_lead_time
                WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
            """,
            'name': 'KPI2: リードタイム遵守率',
            'unit': '%',
            'icon': '⏱️'
        },
        {
            'query': """
                SELECT
                    ROUND(AVG(logistics_cost_ratio), 2) as value,
                    ROUND(AVG(achievement_rate), 1) as achievement,
                    5.0 as target,
                    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
                    COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
                FROM gold_kpi_logistics_cost_ratio
                WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
            """,
            'name': 'KPI3: 物流コスト比率',
            'unit': '%',
            'icon': '🚚'
        },
        {
            'query': """
                SELECT
                    ROUND(AVG(cost_reduction_rate), 2) as value,
                    ROUND(AVG(achievement_rate), 1) as achievement,
                    3.0 as target,
                    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
                    COUNT(CASE WHEN cost_reduction_rate < 0 THEN 1 END) as warning
                FROM gold_kpi_indirect_material_cost_reduction
                WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
            """,
            'name': 'KPI4: 間接材コスト削減率',
            'unit': '%',
            'icon': '💰'
        },
        {
            'query': """
                SELECT
                    ROUND(AVG(cash_conversion_cycle), 1) as value,
                    ROUND(AVG(achievement_rate), 1) as achievement,
                    60.0 as target,
                    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) as excellent,
                    COUNT(CASE WHEN evaluation IN ('要注意', '要改善') THEN 1 END) as warning
                FROM gold_kpi_cash_conversion_cycle
                WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
            """,
            'name': 'KPI5: キャッシュコンバージョンサイクル',
            'unit': '日',
            'icon': '💵'
        }
    ]
    
    html.append('<div class="kpi-cards">')
    
    for kpi in kpis:
        data = pd.read_sql(kpi['query'], conn).iloc[0]
        
        # カードのクラス判定
        card_class = 'kpi-card'
        status_class = 'status-good'
        status_text = '良好'
        
        if data['achievement'] >= 100:
            card_class += ' excellent'
            status_class = 'status-excellent'
            status_text = '優良'
        elif data['warning'] > 0:
            card_class += ' warning'
            status_class = 'status-warning'
            status_text = '要改善'
        
        html.append(f"""
        <div class="{card_class}">
            <div class="kpi-card-title">{kpi['icon']} {kpi['name']}</div>
            <div class="kpi-value">{data['value']:.2f}<span style="font-size:0.5em">{kpi['unit']}</span></div>
            <div class="kpi-target">目標: {data['target']:.1f}{kpi['unit']}</div>
            <div class="achievement-bar">
                <div class="achievement-fill" style="width: {min(data['achievement'], 100):.0f}%"></div>
            </div>
            <div style="margin-top:10px; font-size:0.9em;">
                達成率: {data['achievement']:.1f}%
            </div>
            <div class="kpi-status {status_class}">{status_text}</div>
            <div style="margin-top:10px; font-size:0.85em;">
                優良: {data['excellent']}件 / 要改善: {data['warning']}件
            </div>
        </div>
        """)
    
    html.append('</div>')
    html.append('</div></div>')
    
    return ''.join(html)

def create_location_comparison_html(conn):
    """拠点別比較のHTML生成"""
    html = []
    
    html.append("""
    <div class="section">
        <h2 class="section-title"><span class="icon">🏭</span>拠点別パフォーマンス比較</h2>
    """)
    
    df = pd.read_sql("""
        WITH latest_month AS (
            SELECT MAX(year_month) as ym FROM gold_kpi_inventory_turnover
        )
        SELECT
            i.location_name,
            ROUND(i.inventory_turnover_ratio, 2) as inventory,
            i.evaluation as inv_eval,
            ROUND(l.logistics_cost_ratio, 2) as logistics,
            l.evaluation as log_eval,
            ROUND(c.cash_conversion_cycle, 1) as ccc,
            c.evaluation as ccc_eval
        FROM gold_kpi_inventory_turnover i, latest_month
        JOIN gold_kpi_logistics_cost_ratio l ON i.location_key = l.location_key AND i.year_month = l.year_month
        JOIN gold_kpi_cash_conversion_cycle c ON i.location_key = c.location_key AND i.year_month = c.year_month
        WHERE i.year_month = latest_month.ym
          AND i.product_category = (SELECT product_category FROM gold_kpi_inventory_turnover WHERE year_month = latest_month.ym LIMIT 1)
        ORDER BY i.location_name
    """, conn)
    
    html.append("""
    <table>
        <thead>
            <tr>
                <th>順位</th>
                <th>拠点名</th>
                <th>在庫回転率</th>
                <th>評価</th>
                <th>物流コスト比率</th>
                <th>評価</th>
                <th>CCC</th>
                <th>評価</th>
                <th>総合評価</th>
            </tr>
        </thead>
        <tbody>
    """)
    
    for idx, row in df.iterrows():
        rank_class = 'rank top3' if idx < 3 else 'rank'
        
        # 総合評価
        eval_score = 0
        for eval_col in ['inv_eval', 'log_eval', 'ccc_eval']:
            if row[eval_col] == '優良':
                eval_score += 3
            elif row[eval_col] == '良好':
                eval_score += 2
            elif row[eval_col] == '要改善':
                eval_score += 1
        
        overall = 'A評価' if eval_score >= 7 else 'B評価' if eval_score >= 4 else 'C評価'
        overall_class = 'status-excellent' if eval_score >= 7 else 'status-good' if eval_score >= 4 else 'status-improvement'
        
        html.append(f"""
        <tr>
            <td><span class="{rank_class}">{idx+1}</span></td>
            <td><strong>{row['location_name']}</strong></td>
            <td>{row['inventory']:.2f}</td>
            <td><span class="kpi-status status-{row['inv_eval'].replace('優良', 'excellent').replace('良好', 'good').replace('要改善', 'improvement').replace('要注意', 'warning')}">{row['inv_eval']}</span></td>
            <td>{row['logistics']:.2f}%</td>
            <td><span class="kpi-status status-{row['log_eval'].replace('優良', 'excellent').replace('良好', 'good').replace('要改善', 'improvement').replace('要注意', 'warning')}">{row['log_eval']}</span></td>
            <td>{row['ccc']:.1f}日</td>
            <td><span class="kpi-status status-{row['ccc_eval'].replace('優良', 'excellent').replace('良好', 'good').replace('要改善', 'improvement').replace('要注意', 'warning')}">{row['ccc_eval']}</span></td>
            <td><span class="kpi-status {overall_class}">{overall}</span></td>
        </tr>
        """)
    
    html.append("""
        </tbody>
    </table>
    </div>
    """)
    
    return ''.join(html)

def create_supplier_ranking_html(conn):
    """サプライヤーランキングのHTML生成"""
    html = []
    
    html.append("""
    <div class="section">
        <h2 class="section-title"><span class="icon">🤝</span>サプライヤーランキング TOP10</h2>
    """)
    
    df = pd.read_sql("""
        SELECT
            supplier_name,
            material_category,
            total_orders,
            ROUND(lead_time_adherence_rate, 2) as adherence,
            evaluation
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
          AND total_orders >= 10
        ORDER BY lead_time_adherence_rate DESC
        LIMIT 10
    """, conn)
    
    html.append("""
    <table>
        <thead>
            <tr>
                <th>順位</th>
                <th>サプライヤー名</th>
                <th>材料カテゴリ</th>
                <th>総注文数</th>
                <th>遵守率</th>
                <th>評価</th>
            </tr>
        </thead>
        <tbody>
    """)
    
    for idx, row in df.iterrows():
        rank_class = 'rank top3' if idx < 3 else 'rank'
        
        html.append(f"""
        <tr>
            <td><span class="{rank_class}">{idx+1}</span></td>
            <td><strong>{row['supplier_name']}</strong></td>
            <td>{row['material_category']}</td>
            <td>{row['total_orders']}</td>
            <td>{row['adherence']:.2f}%</td>
            <td><span class="kpi-status status-{row['evaluation'].replace('優良', 'excellent').replace('良好', 'good').replace('要改善', 'improvement').replace('要注意', 'warning')}">{row['evaluation']}</span></td>
        </tr>
        """)
    
    html.append("""
        </tbody>
    </table>
    </div>
    """)
    
    return ''.join(html)

def create_action_items_html(conn):
    """アクションアイテムのHTML生成"""
    html = []
    
    html.append("""
    <div class="section">
        <h2 class="section-title"><span class="icon">⚡</span>優先アクション項目</h2>
    """)
    
    df = pd.read_sql("""
        SELECT
            'KPI1_在庫回転率' as kpi,
            location_name as target,
            ROUND(inventory_turnover_ratio, 2) as current_value,
            ROUND(target_turnover, 2) as target_value,
            evaluation,
            action_recommendation
        FROM gold_kpi_inventory_turnover
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
          AND evaluation IN ('要注意', '要改善')
        
        UNION ALL
        
        SELECT
            'KPI2_リードタイム' as kpi,
            supplier_name as target,
            ROUND(lead_time_adherence_rate, 2) as current_value,
            ROUND(target_adherence_rate, 2) as target_value,
            evaluation,
            action_recommendation
        FROM gold_kpi_procurement_lead_time
        WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
          AND evaluation IN ('要注意', '要改善')
        
        ORDER BY evaluation DESC
        LIMIT 10
    """, conn)
    
    if len(df) == 0:
        html.append('<p style="text-align:center; padding:40px; color:#4caf50; font-size:1.2em;">✅ 優先的な改善項目はありません</p>')
    else:
        for _, row in df.iterrows():
            priority_class = 'high-priority' if row['evaluation'] == '要注意' else ''
            
            html.append(f"""
            <div class="action-item {priority_class}">
                <div class="action-item-title">
                    {row['kpi']} - {row['target']}
                    <span class="kpi-status status-{row['evaluation'].replace('要注意', 'warning').replace('要改善', 'improvement')}">{row['evaluation']}</span>
                </div>
                <div class="action-item-description">
                    <strong>現状:</strong> {row['current_value']:.2f} → <strong>目標:</strong> {row['target_value']:.2f}<br>
                    <strong>アクション:</strong> {row['action_recommendation']}
                </div>
            </div>
            """)
    
    html.append('</div>')
    
    return ''.join(html)

def create_html_footer():
    """HTMLフッターを作成"""
    return f"""
    </div>
    
    <div class="footer">
        <p><strong>KPIモニタリングレポート</strong></p>
        <p>生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
        <p>データソース: analytics.db (SQLite)</p>
        <p style="margin-top:15px; font-size:0.9em;">
            このレポートは自動生成されています。<br>
            詳細な分析が必要な場合は、CSVエクスポートデータをご利用ください。
        </p>
    </div>
    
</body>
</html>
"""

def main():
    """メイン処理"""
    print(f"\n{'='*70}")
    print("  KPIモニタリングレポート HTML生成")
    print(f"{'='*70}\n")
    
    # レポートディレクトリ作成
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # データベース接続
    print(f"データベースに接続: {DATABASE_PATH}")
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        print("HTMLレポートを生成中...\n")
        
        # HTML生成
        html_parts = []
        
        print("  ✓ ヘッダー作成")
        html_parts.append(create_html_header())
        
        print("  ✓ エグゼクティブサマリー作成")
        html_parts.append(create_executive_summary_html(conn))
        
        print("  ✓ 拠点別比較作成")
        html_parts.append(create_location_comparison_html(conn))
        
        print("  ✓ サプライヤーランキング作成")
        html_parts.append(create_supplier_ranking_html(conn))
        
        print("  ✓ アクション項目作成")
        html_parts.append(create_action_items_html(conn))
        
        print("  ✓ フッター作成")
        html_parts.append(create_html_footer())
        
        # ファイル保存
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(''.join(html_parts))
        
        print(f"\n{'='*70}")
        print("✓ HTMLレポートの生成が完了しました！")
        print(f"  保存先: {REPORT_PATH}")
        print(f"  サイズ: {REPORT_PATH.stat().st_size / 1024:.1f} KB")
        print(f"{'='*70}\n")
        
        # ブラウザで開く
        print("ブラウザでレポートを開きますか? (Y/n): ", end='')
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
