/*
==================================================
  アクションプラン抽出クエリ
==================================================
  目的: 改善が必要な項目を自動抽出
  用途: 月次レビュー資料、経営会議報告
*/

-- 【優先度HIGH】要注意項目の抽出（最新月）
SELECT
    'KPI1: 在庫回転率' AS KPI名,
    location_name AS 対象,
    ROUND(inventory_turnover_ratio, 2) AS 現状値,
    ROUND(target_turnover, 2) AS 目標値,
    evaluation AS 評価,
    action_recommendation AS アクションプラン,
    roic_contribution AS ROIC貢献
FROM gold_kpi_inventory_turnover
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
  AND evaluation IN ('要注意', '要改善')

UNION ALL

SELECT
    'KPI2: リードタイム遵守率' AS KPI名,
    supplier_name || ' - ' || material_category AS 対象,
    ROUND(lead_time_adherence_rate, 2) AS 現状値,
    ROUND(target_adherence_rate, 2) AS 目標値,
    evaluation AS 評価,
    action_recommendation AS アクションプラン,
    roic_contribution AS ROIC貢献
FROM gold_kpi_procurement_lead_time
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
  AND evaluation IN ('要注意', '要改善')

UNION ALL

SELECT
    'KPI3: 物流コスト比率' AS KPI名,
    location_name AS 対象,
    ROUND(logistics_cost_ratio, 2) AS 現状値,
    ROUND(target_ratio, 2) AS 目標値,
    evaluation AS 評価,
    action_recommendation AS アクションプラン,
    roic_contribution AS ROIC貢献
FROM gold_kpi_logistics_cost_ratio
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
  AND evaluation IN ('要注意', '要改善')

UNION ALL

SELECT
    'KPI4: 間接材コスト削減率' AS KPI名,
    supplier_name || ' - ' || material_category AS 対象,
    ROUND(cost_reduction_rate, 2) AS 現状値,
    ROUND(target_reduction_rate, 2) AS 目標値,
    evaluation AS 評価,
    action_recommendation AS アクションプラン,
    roic_contribution AS ROIC貢献
FROM gold_kpi_indirect_material_cost_reduction
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
  AND evaluation IN ('要注意', '要改善')

UNION ALL

SELECT
    'KPI5: CCC' AS KPI名,
    location_name AS 対象,
    ROUND(cash_conversion_cycle, 1) AS 現状値,
    ROUND(target_ccc, 1) AS 目標値,
    evaluation AS 評価,
    action_recommendation AS アクションプラン,
    roic_contribution AS ROIC貢献
FROM gold_kpi_cash_conversion_cycle
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
  AND evaluation IN ('要注意', '要改善');

-- 【改善トレンド】前月比で悪化している項目
WITH current_month AS (
    SELECT MAX(year_month) AS ym FROM gold_kpi_inventory_turnover
),
previous_month AS (
    SELECT
        MAX(year_month) AS ym
    FROM gold_kpi_inventory_turnover
    WHERE year_month < (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
)
SELECT
    'KPI1: 在庫回転率' AS KPI名,
    c.location_name AS 対象,
    ROUND(c.inventory_turnover_ratio, 2) AS 当月,
    ROUND(p.inventory_turnover_ratio, 2) AS 前月,
    ROUND(c.inventory_turnover_ratio - p.inventory_turnover_ratio, 2) AS 差分,
    CASE
        WHEN c.inventory_turnover_ratio < p.inventory_turnover_ratio THEN '悪化'
        ELSE '改善'
    END AS トレンド
FROM gold_kpi_inventory_turnover c
JOIN gold_kpi_inventory_turnover p
    ON c.location_key = p.location_key
    AND c.product_category = p.product_category
JOIN current_month cm ON c.year_month = cm.ym
JOIN previous_month pm ON p.year_month = pm.ym
WHERE c.inventory_turnover_ratio < p.inventory_turnover_ratio
ORDER BY 差分 ASC
LIMIT 10;

-- 【ポジティブ事例】優良評価の拠点/サプライヤー
SELECT
    'KPI1: 在庫回転率' AS KPI名,
    location_name AS ベストプラクティス拠点,
    ROUND(inventory_turnover_ratio, 2) AS KPI値,
    ROUND(achievement_rate, 1) || '%' AS 達成率,
    '在庫管理手法を他拠点に展開' AS 水平展開アクション
FROM gold_kpi_inventory_turnover
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
  AND evaluation = '優良'
ORDER BY inventory_turnover_ratio DESC
LIMIT 3

UNION ALL

SELECT
    'KPI2: リードタイム遵守率' AS KPI名,
    supplier_name AS ベストプラクティス拠点,
    ROUND(lead_time_adherence_rate, 2) AS KPI値,
    ROUND(achievement_rate, 1) || '%' AS 達成率,
    'サプライヤー管理手法を共有' AS 水平展開アクション
FROM gold_kpi_procurement_lead_time
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
  AND evaluation = '優良'
ORDER BY lead_time_adherence_rate DESC
LIMIT 3;

-- 【財務インパクト試算】改善余地の定量化
WITH latest_month AS (
    SELECT MAX(year_month) AS ym FROM gold_kpi_inventory_turnover
),
inventory_opportunity AS (
    SELECT
        SUM(avg_inventory_value) AS current_inventory,
        SUM(avg_inventory_value * (target_turnover / inventory_turnover_ratio - 1)) AS reduction_opportunity
    FROM gold_kpi_inventory_turnover, latest_month
    WHERE year_month = ym
      AND inventory_turnover_ratio < target_turnover
),
logistics_opportunity AS (
    SELECT
        SUM(total_logistics_cost) AS current_cost,
        SUM(total_logistics_cost * (logistics_cost_ratio / target_ratio - 1)) AS reduction_opportunity
    FROM gold_kpi_logistics_cost_ratio, latest_month
    WHERE year_month = ym
      AND logistics_cost_ratio > target_ratio
),
procurement_opportunity AS (
    SELECT
        SUM(total_savings) AS realized_savings
    FROM gold_kpi_indirect_material_cost_reduction, latest_month
    WHERE year_month = ym
      AND cost_reduction_rate >= 0
)
SELECT
    '在庫削減余地（百万円）' AS 項目,
    ROUND(reduction_opportunity / 1000000, 2) AS 金額_百万円
FROM inventory_opportunity

UNION ALL

SELECT
    '物流コスト削減余地（百万円）' AS 項目,
    ROUND(reduction_opportunity / 1000000, 2) AS 金額_百万円
FROM logistics_opportunity

UNION ALL

SELECT
    '間接材コスト削減実績（百万円）' AS 項目,
    ROUND(realized_savings / 1000000, 2) AS 金額_百万円
FROM procurement_opportunity

UNION ALL

SELECT
    '合計改善機会（百万円）' AS 項目,
    ROUND((i.reduction_opportunity + l.reduction_opportunity + p.realized_savings) / 1000000, 2) AS 金額_百万円
FROM inventory_opportunity i, logistics_opportunity l, procurement_opportunity p;
