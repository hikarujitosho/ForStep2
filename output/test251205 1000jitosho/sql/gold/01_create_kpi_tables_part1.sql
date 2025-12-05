-- ================================================================================
-- Goldレイヤー: KPIテーブル作成
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- ROIC改善に直結する5つのKPI
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- KPI 1: 在庫回転率 (Inventory Turnover Ratio)
-- ROIC貢献: 投下資本削減（在庫圧縮）
-- ================================================================================
CREATE TABLE gold.kpi_inventory_turnover (
    kpi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    product_key INT,
    location_key INT,
    item_group VARCHAR(100),
    item_hierarchy VARCHAR(50),
    
    -- 在庫指標
    avg_inventory_value DECIMAL(18,2),  -- 平均在庫金額
    beginning_inventory_value DECIMAL(18,2),  -- 期首在庫
    ending_inventory_value DECIMAL(18,2),  -- 期末在庫
    
    -- 売上原価（COGS）
    cogs_amount DECIMAL(18,2),  -- 売上原価
    sales_quantity DECIMAL(18,4),  -- 販売数量
    
    -- KPI算出値
    inventory_turnover_ratio DECIMAL(10,4),  -- 在庫回転率 = COGS / 平均在庫
    inventory_turnover_days DECIMAL(10,2),  -- 在庫回転日数 = 365 / 回転率
    
    -- 改善分析用
    stock_out_days INT DEFAULT 0,  -- 欠品日数
    dead_stock_flag BIT DEFAULT 0,  -- デッドストックフラグ（90日以上滞留）
    inventory_health_status VARCHAR(20),  -- OPTIMAL, OVERSTOCK, UNDERSTOCK
    
    calculation_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_kpi_inv_month ON gold.kpi_inventory_turnover(year_month);
CREATE INDEX idx_kpi_inv_product ON gold.kpi_inventory_turnover(product_key);
CREATE INDEX idx_kpi_inv_location ON gold.kpi_inventory_turnover(location_key);
CREATE INDEX idx_kpi_inv_group ON gold.kpi_inventory_turnover(item_group);

-- データ投入
INSERT INTO gold.kpi_inventory_turnover (
    year_month, product_key, location_key, item_group, item_hierarchy,
    avg_inventory_value, beginning_inventory_value, ending_inventory_value,
    cogs_amount, sales_quantity, inventory_turnover_ratio, 
    inventory_turnover_days, dead_stock_flag, inventory_health_status
)
SELECT
    d.year_month,
    inv.product_key,
    inv.location_key,
    p.item_group,
    p.item_hierarchy,
    
    -- 平均在庫金額
    AVG(inv.inventory_value) AS avg_inventory_value,
    
    -- 期首在庫
    MAX(CASE WHEN inv.snapshot_date = MIN(inv.snapshot_date) OVER (PARTITION BY d.year_month, inv.product_key, inv.location_key)
             THEN inv.inventory_value END) AS beginning_inventory_value,
    
    -- 期末在庫
    MAX(CASE WHEN inv.snapshot_date = MAX(inv.snapshot_date) OVER (PARTITION BY d.year_month, inv.product_key, inv.location_key)
             THEN inv.inventory_value END) AS ending_inventory_value,
    
    -- 売上原価（簡易的に販売金額×0.7と仮定）
    COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0) AS cogs_amount,
    
    -- 販売数量
    COALESCE(SUM(so.quantity), 0) AS sales_quantity,
    
    -- 在庫回転率
    CASE 
        WHEN AVG(inv.inventory_value) > 0 
        THEN COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0) / AVG(inv.inventory_value)
        ELSE 0 
    END AS inventory_turnover_ratio,
    
    -- 在庫回転日数
    CASE 
        WHEN COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0) > 0 
        THEN 365.0 * AVG(inv.inventory_value) / (COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0))
        ELSE 999 
    END AS inventory_turnover_days,
    
    -- デッドストックフラグ（90日以上回転していない）
    CASE 
        WHEN COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0) > 0 
             AND 365.0 * AVG(inv.inventory_value) / (COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0)) > 90
        THEN 1
        ELSE 0 
    END AS dead_stock_flag,
    
    -- 在庫健全性ステータス
    CASE 
        WHEN COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0) = 0 THEN 'DEAD_STOCK'
        WHEN 365.0 * AVG(inv.inventory_value) / (COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0)) < 30 THEN 'OPTIMAL'
        WHEN 365.0 * AVG(inv.inventory_value) / (COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0)) BETWEEN 30 AND 60 THEN 'NORMAL'
        WHEN 365.0 * AVG(inv.inventory_value) / (COALESCE(SUM(so.line_total_ex_tax) * 0.7, 0)) > 60 THEN 'OVERSTOCK'
        ELSE 'UNKNOWN'
    END AS inventory_health_status

FROM silver.fact_inventory_daily inv
INNER JOIN silver.dim_date d ON inv.date_key = d.date_key
INNER JOIN silver.dim_product p ON inv.product_key = p.product_key
LEFT JOIN silver.fact_sales_order so 
    ON inv.product_key = so.product_key 
    AND d.year_month = FORMAT(so.order_date, 'yyyy-MM')
    AND inv.location_key = so.location_key
GROUP BY d.year_month, inv.product_key, inv.location_key, p.item_group, p.item_hierarchy;

GO

-- ================================================================================
-- KPI 2: 調達リードタイム遵守率 (Procurement Lead Time Adherence Rate)
-- ROIC貢献: 投下資本削減（安全在庫削減）+ NOPAT向上（緊急調達コスト削減）
-- ================================================================================
CREATE TABLE gold.kpi_procurement_lead_time (
    kpi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    supplier_key INT,
    location_key INT,
    material_category VARCHAR(100),
    material_type VARCHAR(50),
    account_group VARCHAR(100),
    
    -- 発注件数
    total_orders INT,
    on_time_orders INT,
    delayed_orders INT,
    
    -- 遵守率
    lead_time_adherence_rate DECIMAL(5,4),  -- 期日内納品率
    
    -- リードタイム分析
    avg_planned_lead_time_days DECIMAL(10,2),
    avg_actual_lead_time_days DECIMAL(10,2),
    avg_lead_time_variance_days DECIMAL(10,2),  -- 実績 - 計画
    
    -- 金額影響
    total_order_value DECIMAL(18,2),
    delayed_order_value DECIMAL(18,2),
    
    -- 緊急対応コスト（推定）
    estimated_expedite_cost DECIMAL(18,2),
    
    -- サプライヤー評価
    supplier_performance_grade VARCHAR(10),  -- A/B/C/D
    
    calculation_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (supplier_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_kpi_lt_month ON gold.kpi_procurement_lead_time(year_month);
CREATE INDEX idx_kpi_lt_supplier ON gold.kpi_procurement_lead_time(supplier_key);
CREATE INDEX idx_kpi_lt_material_type ON gold.kpi_procurement_lead_time(material_type);

-- データ投入
INSERT INTO gold.kpi_procurement_lead_time (
    year_month, supplier_key, location_key, material_category, 
    material_type, account_group, total_orders, on_time_orders, 
    delayed_orders, lead_time_adherence_rate, avg_planned_lead_time_days,
    avg_actual_lead_time_days, avg_lead_time_variance_days,
    total_order_value, delayed_order_value, estimated_expedite_cost,
    supplier_performance_grade
)
SELECT
    d.year_month,
    p.supplier_key,
    p.location_key,
    p.material_category,
    p.material_type,
    p.account_group,
    
    COUNT(*) AS total_orders,
    SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS on_time_orders,
    SUM(CASE WHEN p.is_on_time = 0 THEN 1 ELSE 0 END) AS delayed_orders,
    
    -- 遵守率
    CAST(SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(*), 0) AS lead_time_adherence_rate,
    
    -- リードタイム
    AVG(CAST(DATEDIFF(DAY, p.order_date, p.expected_delivery_date) AS DECIMAL(10,2))) AS avg_planned_lead_time_days,
    AVG(CAST(p.lead_time_days AS DECIMAL(10,2))) AS avg_actual_lead_time_days,
    AVG(CAST(p.lead_time_variance_days AS DECIMAL(10,2))) AS avg_lead_time_variance_days,
    
    -- 金額
    SUM(p.line_total_ex_tax) AS total_order_value,
    SUM(CASE WHEN p.is_on_time = 0 THEN p.line_total_ex_tax ELSE 0 END) AS delayed_order_value,
    
    -- 緊急配送コスト（遅延金額の5%と推定）
    SUM(CASE WHEN p.is_on_time = 0 THEN p.line_total_ex_tax ELSE 0 END) * 0.05 AS estimated_expedite_cost,
    
    -- サプライヤーグレード
    CASE 
        WHEN CAST(SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(*), 0) >= 0.95 THEN 'A'
        WHEN CAST(SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(*), 0) >= 0.85 THEN 'B'
        WHEN CAST(SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(*), 0) >= 0.70 THEN 'C'
        ELSE 'D'
    END AS supplier_performance_grade

FROM silver.fact_procurement p
INNER JOIN silver.dim_date d ON p.order_date_key = d.date_key
WHERE p.supplier_key IS NOT NULL
GROUP BY d.year_month, p.supplier_key, p.location_key, p.material_category, p.material_type, p.account_group;

GO

-- ================================================================================
-- KPI 3: 物流コスト対売上高比率 (Logistics Cost to Sales Ratio)
-- ROIC貢献: NOPAT向上（物流コスト削減）
-- ================================================================================
CREATE TABLE gold.kpi_logistics_cost_ratio (
    kpi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    location_key INT,
    transportation_mode VARCHAR(50),
    carrier_name NVARCHAR(100),
    
    -- 物流コスト
    freight_cost DECIMAL(18,2),
    expedite_cost DECIMAL(18,2),
    handling_cost DECIMAL(18,2),
    total_logistics_cost DECIMAL(18,2),
    
    -- 売上
    total_sales_amount DECIMAL(18,2),
    total_shipment_quantity DECIMAL(18,4),
    
    -- KPI算出値
    logistics_cost_to_sales_ratio DECIMAL(5,4),  -- 物流コスト / 売上
    cost_per_unit DECIMAL(18,4),  -- 物流コスト / 出荷数量
    cost_per_shipment DECIMAL(18,2),
    
    -- 輸送効率
    total_shipments INT,
    delayed_shipments INT,
    delay_rate DECIMAL(5,4),
    
    -- 緊急配送比率
    expedite_cost_ratio DECIMAL(5,4),  -- 緊急費用 / 総物流費用
    
    -- 効率評価
    efficiency_grade VARCHAR(10),  -- EXCELLENT/GOOD/AVERAGE/POOR
    
    calculation_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_kpi_log_month ON gold.kpi_logistics_cost_ratio(year_month);
CREATE INDEX idx_kpi_log_location ON gold.kpi_logistics_cost_ratio(location_key);
CREATE INDEX idx_kpi_log_mode ON gold.kpi_logistics_cost_ratio(transportation_mode);

-- データ投入
INSERT INTO gold.kpi_logistics_cost_ratio (
    year_month, location_key, transportation_mode, carrier_name,
    freight_cost, expedite_cost, handling_cost, total_logistics_cost,
    total_sales_amount, total_shipment_quantity, logistics_cost_to_sales_ratio,
    cost_per_unit, cost_per_shipment, total_shipments, delayed_shipments,
    delay_rate, expedite_cost_ratio, efficiency_grade
)
SELECT
    d.year_month,
    COALESCE(s.location_key, tc.location_key) AS location_key,
    s.transportation_mode,
    s.carrier_name,
    
    -- コスト集計
    SUM(CASE WHEN tc.cost_type = 'freight' THEN tc.cost_amount ELSE 0 END) AS freight_cost,
    SUM(CASE WHEN tc.cost_type = 'expedite' THEN tc.cost_amount ELSE 0 END) AS expedite_cost,
    SUM(CASE WHEN tc.cost_type NOT IN ('freight', 'expedite') THEN tc.cost_amount ELSE 0 END) AS handling_cost,
    SUM(tc.cost_amount) AS total_logistics_cost,
    
    -- 売上（出荷ベース）
    SUM(so.line_total_ex_tax) AS total_sales_amount,
    SUM(s.quantity) AS total_shipment_quantity,
    
    -- コスト比率
    CASE 
        WHEN SUM(so.line_total_ex_tax) > 0 
        THEN SUM(tc.cost_amount) / SUM(so.line_total_ex_tax)
        ELSE 0 
    END AS logistics_cost_to_sales_ratio,
    
    -- 単位あたりコスト
    CASE 
        WHEN SUM(s.quantity) > 0 
        THEN SUM(tc.cost_amount) / SUM(s.quantity)
        ELSE 0 
    END AS cost_per_unit,
    
    -- 出荷あたりコスト
    CASE 
        WHEN COUNT(DISTINCT s.shipment_id) > 0 
        THEN SUM(tc.cost_amount) / COUNT(DISTINCT s.shipment_id)
        ELSE 0 
    END AS cost_per_shipment,
    
    -- 出荷件数
    COUNT(DISTINCT s.shipment_id) AS total_shipments,
    SUM(CASE WHEN s.is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_shipments,
    
    -- 遅延率
    CAST(SUM(CASE WHEN s.is_delayed = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,4)) / NULLIF(COUNT(DISTINCT s.shipment_id), 0) AS delay_rate,
    
    -- 緊急配送比率
    SUM(CASE WHEN tc.cost_type = 'expedite' THEN tc.cost_amount ELSE 0 END) / NULLIF(SUM(tc.cost_amount), 0) AS expedite_cost_ratio,
    
    -- 効率評価
    CASE 
        WHEN SUM(tc.cost_amount) / NULLIF(SUM(so.line_total_ex_tax), 0) < 0.03 THEN 'EXCELLENT'
        WHEN SUM(tc.cost_amount) / NULLIF(SUM(so.line_total_ex_tax), 0) < 0.05 THEN 'GOOD'
        WHEN SUM(tc.cost_amount) / NULLIF(SUM(so.line_total_ex_tax), 0) < 0.08 THEN 'AVERAGE'
        ELSE 'POOR'
    END AS efficiency_grade

FROM silver.fact_shipment s
INNER JOIN silver.dim_date d ON s.shipment_date_key = d.date_key
LEFT JOIN silver.fact_transportation_cost tc ON s.shipment_id = tc.shipment_id
LEFT JOIN silver.fact_sales_order so ON s.order_id = so.order_id AND s.line_number = so.line_number
GROUP BY d.year_month, COALESCE(s.location_key, tc.location_key), s.transportation_mode, s.carrier_name;

GO

-- 続きは次のファイルに...
PRINT 'Gold層KPIテーブル（1/2）の作成が完了しました。';
PRINT '- kpi_inventory_turnover: 在庫回転率KPI';
PRINT '- kpi_procurement_lead_time: 調達リードタイム遵守率KPI';
PRINT '- kpi_logistics_cost_ratio: 物流コスト対売上高比率KPI';
PRINT '';
PRINT '次のファイル(02_create_kpi_tables_part2.sql)を実行してください。';
