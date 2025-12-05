-- ================================================================================
-- Goldレイヤー: 分析用統合ビュー
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- 統合KPIダッシュボードビュー
-- 全5つのKPIを統合して一覧表示
-- ================================================================================
CREATE VIEW gold.vw_kpi_dashboard AS
SELECT
    'Inventory Turnover' AS kpi_name,
    'KPI1' AS kpi_code,
    '在庫回転率' AS kpi_name_ja,
    '投下資本削減' AS roic_contribution,
    year_month,
    location_key,
    NULL AS supplier_key,
    product_key,
    inventory_turnover_ratio AS kpi_value,
    inventory_turnover_days AS secondary_metric,
    inventory_health_status AS status,
    NULL AS grade,
    calculation_date
FROM gold.kpi_inventory_turnover

UNION ALL

SELECT
    'Procurement Lead Time',
    'KPI2',
    '調達リードタイム遵守率',
    '投下資本削減 & NOPAT向上',
    year_month,
    location_key,
    supplier_key,
    NULL,
    lead_time_adherence_rate,
    avg_lead_time_variance_days,
    CASE 
        WHEN lead_time_adherence_rate >= 0.95 THEN 'EXCELLENT'
        WHEN lead_time_adherence_rate >= 0.85 THEN 'GOOD'
        WHEN lead_time_adherence_rate >= 0.70 THEN 'AVERAGE'
        ELSE 'POOR'
    END,
    supplier_performance_grade,
    calculation_date
FROM gold.kpi_procurement_lead_time

UNION ALL

SELECT
    'Logistics Cost Ratio',
    'KPI3',
    '物流コスト対売上高比率',
    'NOPAT向上',
    year_month,
    location_key,
    NULL,
    NULL,
    logistics_cost_to_sales_ratio,
    expedite_cost_ratio,
    NULL,
    efficiency_grade,
    calculation_date
FROM gold.kpi_logistics_cost_ratio

UNION ALL

SELECT
    'Cost Reduction Rate',
    'KPI4',
    '間接材調達コスト削減率',
    'NOPAT向上',
    year_month,
    location_key,
    supplier_key,
    NULL,
    cost_reduction_rate,
    mro_spend_ratio,
    NULL,
    savings_grade,
    calculation_date
FROM gold.kpi_indirect_material_cost_reduction

UNION ALL

SELECT
    'Cash Conversion Cycle',
    'KPI5',
    'キャッシュ・コンバージョン・サイクル',
    '投下資本削減',
    year_month,
    location_key,
    NULL,
    NULL,
    ccc_days,
    ccc_vs_previous_month,
    CASE WHEN ccc_improvement_flag = 1 THEN 'IMPROVING' ELSE 'STABLE' END,
    ccc_grade,
    calculation_date
FROM gold.kpi_cash_conversion_cycle;

GO

-- ================================================================================
-- 拠点別KPIサマリービュー
-- 各拠点の全KPI状況を横断表示
-- ================================================================================
CREATE VIEW gold.vw_location_kpi_summary AS
WITH location_kpis AS (
    SELECT
        l.location_id,
        l.location_name,
        l.location_type,
        d.year_month,
        
        -- KPI1: 在庫回転率
        AVG(k1.inventory_turnover_days) AS avg_inventory_turnover_days,
        SUM(k1.avg_inventory_value) AS total_inventory_value,
        
        -- KPI2: 調達リードタイム
        AVG(k2.lead_time_adherence_rate) AS avg_lead_time_adherence_rate,
        SUM(k2.estimated_expedite_cost) AS total_expedite_cost,
        
        -- KPI3: 物流コスト
        SUM(k3.total_logistics_cost) AS total_logistics_cost,
        AVG(k3.logistics_cost_to_sales_ratio) AS avg_logistics_cost_ratio,
        
        -- KPI4: 間接材コスト削減
        SUM(k4.total_cost_savings) AS total_cost_savings,
        AVG(k4.cost_reduction_rate) AS avg_cost_reduction_rate,
        
        -- KPI5: CCC
        AVG(k5.ccc_days) AS avg_ccc_days,
        AVG(k5.working_capital) AS avg_working_capital
        
    FROM silver.dim_location l
    CROSS JOIN (SELECT DISTINCT year_month FROM silver.dim_date WHERE year_month >= FORMAT(DATEADD(MONTH, -12, GETDATE()), 'yyyy-MM')) d
    LEFT JOIN gold.kpi_inventory_turnover k1 ON l.location_key = k1.location_key AND d.year_month = k1.year_month
    LEFT JOIN gold.kpi_procurement_lead_time k2 ON l.location_key = k2.location_key AND d.year_month = k2.year_month
    LEFT JOIN gold.kpi_logistics_cost_ratio k3 ON l.location_key = k3.location_key AND d.year_month = k3.year_month
    LEFT JOIN gold.kpi_indirect_material_cost_reduction k4 ON l.location_key = k4.location_key AND d.year_month = k4.year_month
    LEFT JOIN gold.kpi_cash_conversion_cycle k5 ON l.location_key = k5.location_key AND d.year_month = k5.year_month
    GROUP BY l.location_id, l.location_name, l.location_type, d.year_month
)
SELECT
    location_id,
    location_name,
    location_type,
    year_month,
    
    -- KPI値
    avg_inventory_turnover_days,
    total_inventory_value,
    avg_lead_time_adherence_rate,
    total_expedite_cost,
    total_logistics_cost,
    avg_logistics_cost_ratio,
    total_cost_savings,
    avg_cost_reduction_rate,
    avg_ccc_days,
    avg_working_capital,
    
    -- 総合評価スコア（0-100）
    (
        CASE WHEN avg_inventory_turnover_days < 30 THEN 20 WHEN avg_inventory_turnover_days < 60 THEN 15 WHEN avg_inventory_turnover_days < 90 THEN 10 ELSE 5 END +
        CASE WHEN avg_lead_time_adherence_rate >= 0.95 THEN 20 WHEN avg_lead_time_adherence_rate >= 0.85 THEN 15 WHEN avg_lead_time_adherence_rate >= 0.70 THEN 10 ELSE 5 END +
        CASE WHEN avg_logistics_cost_ratio < 0.03 THEN 20 WHEN avg_logistics_cost_ratio < 0.05 THEN 15 WHEN avg_logistics_cost_ratio < 0.08 THEN 10 ELSE 5 END +
        CASE WHEN avg_cost_reduction_rate > 0.10 THEN 20 WHEN avg_cost_reduction_rate > 0.05 THEN 15 WHEN avg_cost_reduction_rate >= 0 THEN 10 ELSE 5 END +
        CASE WHEN avg_ccc_days < 30 THEN 20 WHEN avg_ccc_days < 60 THEN 15 WHEN avg_ccc_days < 90 THEN 10 ELSE 5 END
    ) AS overall_performance_score
    
FROM location_kpis;

GO

-- ================================================================================
-- サプライヤー評価総合ビュー
-- ================================================================================
CREATE VIEW gold.vw_supplier_performance AS
SELECT
    p.partner_id,
    p.partner_name,
    p.partner_category,
    k2.year_month,
    
    -- リードタイム評価
    k2.total_orders,
    k2.on_time_orders,
    k2.lead_time_adherence_rate,
    k2.avg_lead_time_variance_days,
    k2.supplier_performance_grade,
    
    -- コスト削減貢献
    k4.total_cost_savings,
    k4.cost_reduction_rate,
    k4.mro_procurement_value,
    k4.savings_grade,
    
    -- 総合評価
    CASE 
        WHEN k2.supplier_performance_grade = 'A' AND k4.savings_grade IN ('EXCELLENT', 'GOOD') THEN 'STRATEGIC_PARTNER'
        WHEN k2.supplier_performance_grade IN ('A', 'B') THEN 'PREFERRED_SUPPLIER'
        WHEN k2.supplier_performance_grade = 'C' THEN 'CONDITIONAL_SUPPLIER'
        ELSE 'NEEDS_IMPROVEMENT'
    END AS overall_supplier_status

FROM silver.dim_partner p
INNER JOIN gold.kpi_procurement_lead_time k2 ON p.partner_key = k2.supplier_key
LEFT JOIN gold.kpi_indirect_material_cost_reduction k4 ON p.partner_key = k4.supplier_key AND k2.year_month = k4.year_month
WHERE p.partner_type = 'supplier';

GO

-- ================================================================================
-- ROIC改善トレンドビュー
-- 月次でのROIC改善寄与度を可視化
-- ================================================================================
CREATE VIEW gold.vw_roic_improvement_trend AS
SELECT
    d.year_month,
    d.year,
    d.month,
    
    -- 投下資本削減効果（推定）
    SUM(k1.avg_inventory_value) AS total_inventory_capital,
    AVG(k1.inventory_turnover_days) AS avg_inventory_days,
    
    SUM(k5.working_capital) AS total_working_capital,
    AVG(k5.ccc_days) AS avg_ccc_days,
    
    -- 投下資本削減額（前年同月比）
    SUM(k1.avg_inventory_value) - LAG(SUM(k1.avg_inventory_value)) OVER (PARTITION BY d.month ORDER BY d.year) AS inventory_reduction_amount,
    SUM(k5.working_capital) - LAG(SUM(k5.working_capital)) OVER (PARTITION BY d.month ORDER BY d.year) AS working_capital_reduction,
    
    -- NOPAT改善効果（コスト削減）
    SUM(k3.total_logistics_cost) AS total_logistics_cost,
    SUM(k4.total_cost_savings) AS total_procurement_savings,
    SUM(k2.estimated_expedite_cost) AS total_expedite_cost,
    
    -- NOPAT改善額（前年同月比）
    (LAG(SUM(k3.total_logistics_cost)) OVER (PARTITION BY d.month ORDER BY d.year) - SUM(k3.total_logistics_cost)) AS logistics_cost_reduction,
    SUM(k4.total_cost_savings) AS procurement_cost_savings,
    
    -- ROIC改善総合指標
    (
        -- 投下資本削減効果（在庫+運転資本）
        COALESCE(SUM(k1.avg_inventory_value) - LAG(SUM(k1.avg_inventory_value)) OVER (PARTITION BY d.month ORDER BY d.year), 0) +
        COALESCE(SUM(k5.working_capital) - LAG(SUM(k5.working_capital)) OVER (PARTITION BY d.month ORDER BY d.year), 0) +
        -- NOPAT改善効果（コスト削減）
        COALESCE(LAG(SUM(k3.total_logistics_cost)) OVER (PARTITION BY d.month ORDER BY d.year) - SUM(k3.total_logistics_cost), 0) +
        COALESCE(SUM(k4.total_cost_savings), 0)
    ) AS estimated_roic_improvement_amount

FROM (SELECT DISTINCT year_month, year, month FROM silver.dim_date WHERE year >= YEAR(GETDATE()) - 2) d
LEFT JOIN gold.kpi_inventory_turnover k1 ON d.year_month = k1.year_month
LEFT JOIN gold.kpi_procurement_lead_time k2 ON d.year_month = k2.year_month
LEFT JOIN gold.kpi_logistics_cost_ratio k3 ON d.year_month = k3.year_month
LEFT JOIN gold.kpi_indirect_material_cost_reduction k4 ON d.year_month = k4.year_month
LEFT JOIN gold.kpi_cash_conversion_cycle k5 ON d.year_month = k5.year_month
GROUP BY d.year_month, d.year, d.month;

GO

-- ================================================================================
-- ビュー作成完了
-- ================================================================================
PRINT 'Gold層分析用ビューの作成が完了しました。';
PRINT '- vw_kpi_dashboard: 統合KPIダッシュボード';
PRINT '- vw_location_kpi_summary: 拠点別KPIサマリー';
PRINT '- vw_supplier_performance: サプライヤー評価総合';
PRINT '- vw_roic_improvement_trend: ROIC改善トレンド';
PRINT '';
PRINT 'メダリオンアーキテクチャの全SQL作成が完了しました！';
