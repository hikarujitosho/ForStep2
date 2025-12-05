/*
==================================================
  拠点別KPI比較クエリ
==================================================
  目的: 各拠点のパフォーマンスを比較
  用途: 問題拠点の特定、ベストプラクティス共有
*/

-- 拠点別在庫回転率ランキング（最新月）
SELECT
    location_name AS 拠点名,
    ROUND(inventory_turnover_ratio, 2) AS 在庫回転率,
    ROUND(achievement_rate, 1) || '%' AS 達成率,
    evaluation AS 評価,
    action_recommendation AS アクションプラン
FROM gold_kpi_inventory_turnover
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_inventory_turnover)
ORDER BY inventory_turnover_ratio DESC;

-- 拠点別物流コスト比率ランキング（最新月）
SELECT
    location_name AS 拠点名,
    ROUND(logistics_cost_ratio, 2) || '%' AS 物流コスト比率,
    ROUND(total_logistics_cost / 1000000, 2) AS 物流コスト_百万円,
    ROUND(total_sales / 1000000, 2) AS 売上_百万円,
    evaluation AS 評価,
    action_recommendation AS アクションプラン
FROM gold_kpi_logistics_cost_ratio
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_logistics_cost_ratio)
ORDER BY logistics_cost_ratio ASC;

-- 拠点別CCC比較（最新月）
SELECT
    location_name AS 拠点名,
    ROUND(cash_conversion_cycle, 1) AS CCC日数,
    ROUND(dio, 1) AS DIO,
    ROUND(dso, 1) AS DSO,
    ROUND(dpo, 1) AS DPO,
    evaluation AS 評価,
    CASE
        WHEN cash_conversion_cycle < 60 THEN '優秀'
        WHEN cash_conversion_cycle < 90 THEN '標準'
        ELSE '要改善'
    END AS パフォーマンス
FROM gold_kpi_cash_conversion_cycle
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_cash_conversion_cycle)
ORDER BY cash_conversion_cycle ASC;

-- 拠点別総合評価（全KPI統合）
WITH latest_month AS (
    SELECT MAX(year_month) AS ym FROM gold_kpi_inventory_turnover
),
inventory_score AS (
    SELECT
        location_name,
        CASE evaluation
            WHEN '優良' THEN 4
            WHEN '良好' THEN 3
            WHEN '要改善' THEN 2
            ELSE 1
        END AS score
    FROM gold_kpi_inventory_turnover, latest_month
    WHERE year_month = ym
),
logistics_score AS (
    SELECT
        location_name,
        CASE evaluation
            WHEN '優良' THEN 4
            WHEN '良好' THEN 3
            WHEN '要改善' THEN 2
            ELSE 1
        END AS score
    FROM gold_kpi_logistics_cost_ratio, latest_month
    WHERE year_month = ym
),
ccc_score AS (
    SELECT
        location_name,
        CASE evaluation
            WHEN '優良' THEN 4
            WHEN '良好' THEN 3
            WHEN '要改善' THEN 2
            ELSE 1
        END AS score
    FROM gold_kpi_cash_conversion_cycle, latest_month
    WHERE year_month = ym
)
SELECT
    i.location_name AS 拠点名,
    i.score + l.score + c.score AS 総合スコア,
    i.score AS 在庫スコア,
    l.score AS 物流スコア,
    c.score AS CCCスコア,
    CASE
        WHEN i.score + l.score + c.score >= 10 THEN '優秀拠点'
        WHEN i.score + l.score + c.score >= 7 THEN '標準拠点'
        ELSE '要改善拠点'
    END AS 拠点区分
FROM inventory_score i
JOIN logistics_score l ON i.location_name = l.location_name
JOIN ccc_score c ON i.location_name = c.location_name
ORDER BY 総合スコア DESC;
