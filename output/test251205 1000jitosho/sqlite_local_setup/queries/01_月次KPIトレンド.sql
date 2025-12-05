/*
==================================================
  月次KPIトレンド分析クエリ
==================================================
  目的: 5つのKPIの月次推移を一覧表示
  用途: ExcelやPower BIでグラフ化
*/

-- KPI1: 在庫回転率の推移
SELECT
    year_month AS 年月,
    ROUND(AVG(inventory_turnover_ratio), 2) AS 在庫回転率,
    ROUND(AVG(achievement_rate), 1) AS 達成率,
    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) AS 優良拠点数,
    COUNT(*) AS 総拠点数
FROM gold_kpi_inventory_turnover
GROUP BY year_month
ORDER BY year_month;

-- KPI2: 調達リードタイム遵守率の推移
SELECT
    year_month AS 年月,
    ROUND(AVG(lead_time_adherence_rate), 2) AS リードタイム遵守率,
    ROUND(AVG(achievement_rate), 1) AS 達成率,
    COUNT(CASE WHEN evaluation = '優良' THEN 1 END) AS 優良サプライヤー数,
    COUNT(*) AS 総サプライヤー数
FROM gold_kpi_procurement_lead_time
GROUP BY year_month
ORDER BY year_month;

-- KPI3: 物流コスト売上高比率の推移
SELECT
    year_month AS 年月,
    ROUND(AVG(logistics_cost_ratio), 2) AS 物流コスト比率,
    ROUND(AVG(achievement_rate), 1) AS 達成率,
    ROUND(SUM(total_logistics_cost) / 1000000, 2) AS 物流コスト合計_百万円
FROM gold_kpi_logistics_cost_ratio
GROUP BY year_month
ORDER BY year_month;

-- KPI4: 間接材調達コスト削減率の推移
SELECT
    year_month AS 年月,
    ROUND(AVG(cost_reduction_rate), 2) AS 平均削減率,
    ROUND(SUM(total_savings) / 1000000, 2) AS 削減額合計_百万円,
    COUNT(CASE WHEN is_improving = 1 THEN 1 END) AS 改善案件数,
    COUNT(*) AS 総案件数
FROM gold_kpi_indirect_material_cost_reduction
GROUP BY year_month
ORDER BY year_month;

-- KPI5: キャッシュコンバージョンサイクルの推移
SELECT
    year_month AS 年月,
    ROUND(AVG(cash_conversion_cycle), 1) AS CCC日数,
    ROUND(AVG(dio), 1) AS DIO日数,
    ROUND(AVG(dso), 1) AS DSO日数,
    ROUND(AVG(dpo), 1) AS DPO日数,
    ROUND(AVG(achievement_rate), 1) AS 達成率
FROM gold_kpi_cash_conversion_cycle
GROUP BY year_month
ORDER BY year_month;

-- 全KPI総合サマリー（最新月）
SELECT
    'KPI1: 在庫回転率' AS KPI名,
    ROUND(AVG(inventory_turnover_ratio), 2) AS KPI値,
    ROUND(AVG(achievement_rate), 1) || '%' AS 達成率,
    MAX(evaluation) AS 評価
FROM gold_kpi_inventory_turnover
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)

UNION ALL

SELECT
    'KPI2: 調達リードタイム遵守率' AS KPI名,
    ROUND(AVG(lead_time_adherence_rate), 2) AS KPI値,
    ROUND(AVG(achievement_rate), 1) || '%' AS 達成率,
    MAX(evaluation) AS 評価
FROM gold_kpi_procurement_lead_time
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)

UNION ALL

SELECT
    'KPI3: 物流コスト売上高比率' AS KPI名,
    ROUND(AVG(logistics_cost_ratio), 2) AS KPI値,
    ROUND(AVG(achievement_rate), 1) || '%' AS 達成率,
    MAX(evaluation) AS 評価
FROM gold_kpi_logistics_cost_ratio
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)

UNION ALL

SELECT
    'KPI4: 間接材調達コスト削減率' AS KPI名,
    ROUND(AVG(cost_reduction_rate), 2) AS KPI値,
    ROUND(AVG(achievement_rate), 1) || '%' AS 達成率,
    MAX(evaluation) AS 評価
FROM gold_kpi_indirect_material_cost_reduction
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)

UNION ALL

SELECT
    'KPI5: キャッシュコンバージョンサイクル' AS KPI名,
    ROUND(AVG(cash_conversion_cycle), 1) AS KPI値,
    ROUND(AVG(achievement_rate), 1) || '%' AS 達成率,
    MAX(evaluation) AS 評価
FROM gold_kpi_cash_conversion_cycle
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle);
