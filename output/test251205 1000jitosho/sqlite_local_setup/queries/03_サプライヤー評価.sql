/*
==================================================
  サプライヤー評価クエリ
==================================================
  目的: サプライヤーのパフォーマンスを評価
  用途: サプライヤー選定、契約更新判断
*/

-- サプライヤー別リードタイム遵守率ランキング（最新月）
SELECT
    supplier_name AS サプライヤー名,
    material_category AS 材料カテゴリ,
    account_group AS アカウントグループ,
    total_orders AS 総注文数,
    on_time_deliveries AS 期限内納品数,
    late_deliveries AS 遅延数,
    ROUND(lead_time_adherence_rate, 2) || '%' AS 遵守率,
    ROUND(avg_lead_time_days, 1) AS 平均リードタイム日数,
    ROUND(avg_lead_time_variance_days, 1) AS 平均遅延日数,
    evaluation AS 評価,
    action_recommendation AS アクションプラン
FROM gold_kpi_procurement_lead_time
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_procurement_lead_time)
  AND total_orders >= 5  -- 最低5件以上の注文があるサプライヤー
ORDER BY lead_time_adherence_rate DESC;

-- サプライヤー別コスト削減貢献度（最新月）
SELECT
    supplier_name AS サプライヤー名,
    material_category AS 材料カテゴリ,
    ROUND(cost_reduction_rate, 2) || '%' AS コスト削減率,
    ROUND(baseline_unit_price, 0) AS ベースライン単価,
    ROUND(current_unit_price, 0) AS 現在単価,
    ROUND(total_savings / 1000000, 2) AS 削減額_百万円,
    ROUND(quantity_procured, 0) AS 調達数量,
    evaluation AS 評価,
    CASE
        WHEN cost_reduction_rate >= 5 THEN 'トップサプライヤー'
        WHEN cost_reduction_rate >= 3 THEN '優良サプライヤー'
        WHEN cost_reduction_rate >= 0 THEN '標準サプライヤー'
        ELSE '要見直しサプライヤー'
    END AS サプライヤー区分
FROM gold_kpi_indirect_material_cost_reduction
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
ORDER BY total_savings DESC;

-- サプライヤー総合評価（リードタイム + コスト削減）
WITH latest_month AS (
    SELECT MAX(year_month) AS ym FROM gold_kpi_procurement_lead_time
),
lead_time_performance AS (
    SELECT
        supplier_key,
        supplier_name,
        CASE evaluation
            WHEN '優良' THEN 4
            WHEN '良好' THEN 3
            WHEN '要改善' THEN 2
            ELSE 1
        END AS lt_score,
        ROUND(lead_time_adherence_rate, 2) AS adherence_rate,
        total_orders
    FROM gold_kpi_procurement_lead_time, latest_month
    WHERE year_month = ym
),
cost_performance AS (
    SELECT
        supplier_key,
        CASE evaluation
            WHEN '優良' THEN 4
            WHEN '良好' THEN 3
            WHEN '要改善' THEN 2
            ELSE 1
        END AS cost_score,
        ROUND(cost_reduction_rate, 2) AS reduction_rate,
        ROUND(total_savings / 1000000, 2) AS savings_million
    FROM gold_kpi_indirect_material_cost_reduction, latest_month
    WHERE year_month = ym
)
SELECT
    lt.supplier_name AS サプライヤー名,
    lt.total_orders AS 総注文数,
    lt.adherence_rate || '%' AS リードタイム遵守率,
    COALESCE(cp.reduction_rate, 0) || '%' AS コスト削減率,
    COALESCE(cp.savings_million, 0) AS 削減額_百万円,
    lt.lt_score + COALESCE(cp.cost_score, 0) AS 総合スコア,
    CASE
        WHEN lt.lt_score + COALESCE(cp.cost_score, 0) >= 7 THEN 'Aランク（優先継続）'
        WHEN lt.lt_score + COALESCE(cp.cost_score, 0) >= 5 THEN 'Bランク（継続）'
        WHEN lt.lt_score + COALESCE(cp.cost_score, 0) >= 3 THEN 'Cランク（要改善要請）'
        ELSE 'Dランク（代替検討）'
    END AS サプライヤーランク
FROM lead_time_performance lt
LEFT JOIN cost_performance cp ON lt.supplier_key = cp.supplier_key
ORDER BY 総合スコア DESC;

-- 間接材（MRO）サプライヤー集約候補
SELECT
    material_category AS 材料カテゴリ,
    COUNT(DISTINCT supplier_name) AS サプライヤー数,
    ROUND(SUM(quantity_procured), 0) AS 総調達数量,
    ROUND(AVG(current_unit_price), 0) AS 平均単価,
    ROUND(SUM(total_savings) / 1000000, 2) AS 削減額合計_百万円,
    CASE
        WHEN COUNT(DISTINCT supplier_name) >= 5 THEN '集約推奨（5社以上）'
        WHEN COUNT(DISTINCT supplier_name) >= 3 THEN '集約検討（3-4社）'
        ELSE '集約不要'
    END AS 集約方針
FROM gold_kpi_indirect_material_cost_reduction
WHERE year_month = (SELECT MAX(year_month) FROM gold_kpi_indirect_material_cost_reduction)
  AND account_group = 'MRO'
GROUP BY material_category
ORDER BY サプライヤー数 DESC;
