-- ================================================================================
-- Goldレイヤー: KPIテーブル作成 (Part 2)
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- ROIC改善に直結する5つのKPI (続き)
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- KPI 4: 間接材調達コスト削減率 (Indirect Materials Cost Reduction Rate)
-- ROIC貢献: NOPAT向上（調達コスト削減）
-- ================================================================================
CREATE TABLE gold.kpi_indirect_material_cost_reduction (
    kpi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    supplier_key INT,
    location_key INT,
    material_category VARCHAR(100),
    department_code VARCHAR(50),
    
    -- 調達金額
    total_procurement_value DECIMAL(18,2),
    mro_procurement_value DECIMAL(18,2),
    direct_material_value DECIMAL(18,2),
    
    -- 価格分析
    total_reference_value DECIMAL(18,2),  -- 基準価格総額
    total_actual_value DECIMAL(18,2),  -- 実際価格総額
    total_cost_savings DECIMAL(18,2),  -- 削減額
    
    -- KPI算出値
    cost_reduction_rate DECIMAL(5,4),  -- (基準 - 実際) / 基準
    avg_discount_rate DECIMAL(5,4),
    
    -- MRO比率
    mro_spend_ratio DECIMAL(5,4),  -- MRO / 総調達額
    
    -- 集約効果
    total_suppliers INT,
    total_orders INT,
    avg_order_value DECIMAL(18,2),
    
    -- 購買効率
    spot_purchase_ratio DECIMAL(5,4),  -- スポット購買比率
    savings_grade VARCHAR(10),  -- EXCELLENT/GOOD/AVERAGE/POOR
    
    calculation_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (supplier_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_kpi_mro_month ON gold.kpi_indirect_material_cost_reduction(year_month);
CREATE INDEX idx_kpi_mro_supplier ON gold.kpi_indirect_material_cost_reduction(supplier_key);
CREATE INDEX idx_kpi_mro_dept ON gold.kpi_indirect_material_cost_reduction(department_code);

-- データ投入
INSERT INTO gold.kpi_indirect_material_cost_reduction (
    year_month, supplier_key, location_key, material_category, department_code,
    total_procurement_value, mro_procurement_value, direct_material_value,
    total_reference_value, total_actual_value, total_cost_savings,
    cost_reduction_rate, avg_discount_rate, mro_spend_ratio,
    total_suppliers, total_orders, avg_order_value, spot_purchase_ratio, savings_grade
)
SELECT
    d.year_month,
    p.supplier_key,
    p.location_key,
    p.material_category,
    p.department_code,
    
    -- 調達金額
    SUM(p.line_total_ex_tax) AS total_procurement_value,
    SUM(CASE WHEN p.is_mro = 1 THEN p.line_total_ex_tax ELSE 0 END) AS mro_procurement_value,
    SUM(CASE WHEN p.material_type = 'direct' THEN p.line_total_ex_tax ELSE 0 END) AS direct_material_value,
    
    -- 価格分析
    SUM(p.reference_price_ex_tax * p.quantity) AS total_reference_value,
    SUM(p.unit_price_ex_tax * p.quantity) AS total_actual_value,
    SUM((p.reference_price_ex_tax - p.unit_price_ex_tax) * p.quantity) AS total_cost_savings,
    
    -- コスト削減率
    CASE 
        WHEN SUM(p.reference_price_ex_tax * p.quantity) > 0 
        THEN SUM((p.reference_price_ex_tax - p.unit_price_ex_tax) * p.quantity) / SUM(p.reference_price_ex_tax * p.quantity)
        ELSE 0 
    END AS cost_reduction_rate,
    
    -- 平均割引率
    AVG(CAST(p.discount_amount AS DECIMAL(18,4)) / NULLIF(p.line_total_incl_tax, 0)) AS avg_discount_rate,
    
    -- MRO比率
    SUM(CASE WHEN p.is_mro = 1 THEN p.line_total_ex_tax ELSE 0 END) / NULLIF(SUM(p.line_total_ex_tax), 0) AS mro_spend_ratio,
    
    -- 集約効果
    COUNT(DISTINCT p.supplier_key) AS total_suppliers,
    COUNT(*) AS total_orders,
    AVG(p.line_total_ex_tax) AS avg_order_value,
    
    -- スポット購買比率
    CAST(SUM(CASE WHEN p.purchase_rule = '該当無し' THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(*), 0) AS spot_purchase_ratio,
    
    -- 削減評価
    CASE 
        WHEN SUM((p.reference_price_ex_tax - p.unit_price_ex_tax) * p.quantity) / NULLIF(SUM(p.reference_price_ex_tax * p.quantity), 0) > 0.10 THEN 'EXCELLENT'
        WHEN SUM((p.reference_price_ex_tax - p.unit_price_ex_tax) * p.quantity) / NULLIF(SUM(p.reference_price_ex_tax * p.quantity), 0) > 0.05 THEN 'GOOD'
        WHEN SUM((p.reference_price_ex_tax - p.unit_price_ex_tax) * p.quantity) / NULLIF(SUM(p.reference_price_ex_tax * p.quantity), 0) >= 0 THEN 'AVERAGE'
        ELSE 'POOR'
    END AS savings_grade

FROM silver.fact_procurement p
INNER JOIN silver.dim_date d ON p.order_date_key = d.date_key
WHERE p.reference_price_ex_tax IS NOT NULL
GROUP BY d.year_month, p.supplier_key, p.location_key, p.material_category, p.department_code;

GO

-- ================================================================================
-- KPI 5: キャッシュ・コンバージョン・サイクル (Cash Conversion Cycle)
-- ROIC貢献: 投下資本削減（運転資本最適化）
-- ================================================================================
CREATE TABLE gold.kpi_cash_conversion_cycle (
    kpi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    location_key INT,
    
    -- DIO (Days Inventory Outstanding) - 在庫回転日数
    avg_inventory_value DECIMAL(18,2),
    cogs_amount DECIMAL(18,2),
    dio_days DECIMAL(10,2),  -- 平均在庫 / COGS × 365
    
    -- DSO (Days Sales Outstanding) - 売掛金回収日数
    avg_accounts_receivable DECIMAL(18,2),  -- 平均売掛金
    revenue_amount DECIMAL(18,2),
    dso_days DECIMAL(10,2),  -- 平均売掛金 / 売上 × 365
    
    -- DPO (Days Payable Outstanding) - 買掛金支払日数
    avg_accounts_payable DECIMAL(18,2),  -- 平均買掛金
    purchases_amount DECIMAL(18,2),
    dpo_days DECIMAL(10,2),  -- 平均買掛金 / 仕入 × 365
    
    -- CCC算出
    ccc_days DECIMAL(10,2),  -- DIO + DSO - DPO
    
    -- 運転資本
    working_capital DECIMAL(18,2),  -- 在庫 + 売掛金 - 買掛金
    
    -- 改善トレンド
    ccc_vs_previous_month DECIMAL(10,2),
    ccc_improvement_flag BIT,
    
    -- 評価
    ccc_grade VARCHAR(10),  -- EXCELLENT/GOOD/AVERAGE/POOR
    
    calculation_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_kpi_ccc_month ON gold.kpi_cash_conversion_cycle(year_month);
CREATE INDEX idx_kpi_ccc_location ON gold.kpi_cash_conversion_cycle(location_key);

-- データ投入
WITH monthly_metrics AS (
    SELECT
        d.year_month,
        COALESCE(inv.location_key, so.location_key, pr.location_key) AS location_key,
        
        -- 在庫（月平均）
        AVG(inv.inventory_value) AS avg_inventory_value,
        
        -- 売上原価（売上×0.7と仮定）
        SUM(so.line_total_ex_tax) * 0.7 AS cogs_amount,
        
        -- 売上
        SUM(so.line_total_ex_tax) AS revenue_amount,
        
        -- 売掛金（簡易的に月末売上の30日分と仮定）
        SUM(so.line_total_ex_tax) * 30.0 / 365.0 AS avg_accounts_receivable,
        
        -- 仕入
        SUM(pr.line_total_ex_tax) AS purchases_amount,
        
        -- 買掛金（支払日 - 発注日の平均日数 × 平均仕入額）
        SUM(pr.line_total_ex_tax) * AVG(DATEDIFF(DAY, pr.order_date, pr.payment_date)) / 365.0 AS avg_accounts_payable
        
    FROM silver.dim_date d
    LEFT JOIN silver.fact_inventory_daily inv ON d.date_key = inv.date_key
    LEFT JOIN silver.fact_sales_order so ON d.date_key = so.order_date_key
    LEFT JOIN silver.fact_procurement pr ON d.date_key = pr.order_date_key
    GROUP BY d.year_month, COALESCE(inv.location_key, so.location_key, pr.location_key)
),
ccc_calculated AS (
    SELECT
        year_month,
        location_key,
        avg_inventory_value,
        cogs_amount,
        revenue_amount,
        purchases_amount,
        avg_accounts_receivable,
        avg_accounts_payable,
        
        -- DIO
        CASE WHEN cogs_amount > 0 THEN (avg_inventory_value / cogs_amount) * 365 ELSE 0 END AS dio_days,
        
        -- DSO
        CASE WHEN revenue_amount > 0 THEN (avg_accounts_receivable / revenue_amount) * 365 ELSE 0 END AS dso_days,
        
        -- DPO
        CASE WHEN purchases_amount > 0 THEN (avg_accounts_payable / purchases_amount) * 365 ELSE 0 END AS dpo_days,
        
        -- Working Capital
        avg_inventory_value + avg_accounts_receivable - avg_accounts_payable AS working_capital
        
    FROM monthly_metrics
)
INSERT INTO gold.kpi_cash_conversion_cycle (
    year_month, location_key, avg_inventory_value, cogs_amount,
    avg_accounts_receivable, revenue_amount, dso_days,
    avg_accounts_payable, purchases_amount, dpo_days,
    dio_days, ccc_days, working_capital, ccc_vs_previous_month,
    ccc_improvement_flag, ccc_grade
)
SELECT
    year_month,
    location_key,
    avg_inventory_value,
    cogs_amount,
    avg_accounts_receivable,
    revenue_amount,
    dso_days,
    avg_accounts_payable,
    purchases_amount,
    dpo_days,
    dio_days,
    
    -- CCC計算
    dio_days + dso_days - dpo_days AS ccc_days,
    
    working_capital,
    
    -- 前月との差分
    (dio_days + dso_days - dpo_days) - LAG(dio_days + dso_days - dpo_days) OVER (PARTITION BY location_key ORDER BY year_month) AS ccc_vs_previous_month,
    
    -- 改善フラグ
    CASE 
        WHEN (dio_days + dso_days - dpo_days) < LAG(dio_days + dso_days - dpo_days) OVER (PARTITION BY location_key ORDER BY year_month) 
        THEN 1 ELSE 0 
    END AS ccc_improvement_flag,
    
    -- グレード
    CASE 
        WHEN dio_days + dso_days - dpo_days < 30 THEN 'EXCELLENT'
        WHEN dio_days + dso_days - dpo_days < 60 THEN 'GOOD'
        WHEN dio_days + dso_days - dpo_days < 90 THEN 'AVERAGE'
        ELSE 'POOR'
    END AS ccc_grade

FROM ccc_calculated;

GO

-- ================================================================================
-- Gold層KPIテーブル作成完了
-- ================================================================================
PRINT 'Gold層KPIテーブル（2/2）の作成が完了しました。';
PRINT '- kpi_indirect_material_cost_reduction: 間接材調達コスト削減率KPI';
PRINT '- kpi_cash_conversion_cycle: キャッシュ・コンバージョン・サイクルKPI';
PRINT '';
PRINT '全5つのKPIテーブルが正常に作成されました！';
PRINT '';
PRINT '【ROIC改善への貢献】';
PRINT 'NOPAT向上: ';
PRINT '  - KPI3: 物流コスト削減';
PRINT '  - KPI4: 間接材調達コスト削減';
PRINT '投下資本削減: ';
PRINT '  - KPI1: 在庫削減';
PRINT '  - KPI2: 安全在庫削減';
PRINT '  - KPI5: 運転資本最適化';
