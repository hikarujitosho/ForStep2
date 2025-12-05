-- ================================================================================
-- Silverレイヤー: 集計中間テーブル作成
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- 1. agg_inventory_monthly (月次在庫集計)
-- ================================================================================
CREATE TABLE silver.agg_inventory_monthly (
    agg_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,  -- YYYY-MM
    product_key INT NOT NULL,
    location_key INT NOT NULL,
    avg_inventory_quantity DECIMAL(18,4),
    max_inventory_quantity DECIMAL(18,4),
    min_inventory_quantity DECIMAL(18,4),
    avg_inventory_value DECIMAL(18,2),
    total_inventory_value_month_end DECIMAL(18,2),
    days_in_stock INT,
    source_system VARCHAR(20) DEFAULT 'WMS',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_agg_inventory_month ON silver.agg_inventory_monthly(year_month);
CREATE UNIQUE INDEX idx_agg_inventory_unique ON silver.agg_inventory_monthly(year_month, product_key, location_key);

-- データ投入
INSERT INTO silver.agg_inventory_monthly (
    year_month, product_key, location_key,
    avg_inventory_quantity, max_inventory_quantity, min_inventory_quantity,
    avg_inventory_value, total_inventory_value_month_end
)
SELECT
    d.year_month,
    i.product_key,
    i.location_key,
    AVG(i.inventory_quantity),
    MAX(i.inventory_quantity),
    MIN(i.inventory_quantity),
    AVG(i.inventory_value),
    MAX(CASE WHEN i.snapshot_date = MAX(i.snapshot_date) OVER (PARTITION BY d.year_month, i.product_key, i.location_key) 
             THEN i.inventory_value ELSE NULL END)
FROM silver.fact_inventory_daily i
INNER JOIN silver.dim_date d ON i.date_key = d.date_key
GROUP BY d.year_month, i.product_key, i.location_key;

GO

-- ================================================================================
-- 2. agg_procurement_performance (調達パフォーマンス集計)
-- ================================================================================
CREATE TABLE silver.agg_procurement_performance (
    agg_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    supplier_key INT NOT NULL,
    location_key INT,
    material_type VARCHAR(50),
    total_orders INT,
    total_order_amount DECIMAL(18,2),
    on_time_orders INT,
    delayed_orders INT,
    on_time_rate DECIMAL(5,4),
    avg_lead_time_days DECIMAL(10,2),
    avg_lead_time_variance_days DECIMAL(10,2),
    total_price_variance DECIMAL(18,2),
    avg_discount_rate DECIMAL(5,4),
    source_system VARCHAR(20) DEFAULT 'P2P',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (supplier_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_agg_procurement_month ON silver.agg_procurement_performance(year_month);
CREATE INDEX idx_agg_procurement_supplier ON silver.agg_procurement_performance(supplier_key);
CREATE UNIQUE INDEX idx_agg_procurement_unique ON silver.agg_procurement_performance(year_month, supplier_key, location_key, material_type);

-- データ投入
INSERT INTO silver.agg_procurement_performance (
    year_month, supplier_key, location_key, material_type,
    total_orders, total_order_amount, on_time_orders, delayed_orders,
    on_time_rate, avg_lead_time_days, avg_lead_time_variance_days,
    total_price_variance, avg_discount_rate
)
SELECT
    d.year_month,
    p.supplier_key,
    p.location_key,
    p.material_type,
    COUNT(*),
    SUM(p.line_total_ex_tax),
    SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN p.is_on_time = 0 THEN 1 ELSE 0 END),
    CAST(SUM(CASE WHEN p.is_on_time = 1 THEN 1 ELSE 0 END) AS DECIMAL(18,2)) / NULLIF(COUNT(*), 0),
    AVG(CAST(p.lead_time_days AS DECIMAL(10,2))),
    AVG(CAST(p.lead_time_variance_days AS DECIMAL(10,2))),
    SUM(p.price_variance * p.quantity),
    AVG(CAST(p.discount_amount AS DECIMAL(18,2)) / NULLIF(p.line_total_incl_tax, 0))
FROM silver.fact_procurement p
INNER JOIN silver.dim_date d ON p.order_date_key = d.date_key
WHERE p.supplier_key IS NOT NULL
GROUP BY d.year_month, p.supplier_key, p.location_key, p.material_type;

GO

-- ================================================================================
-- 集計中間テーブル作成完了
-- ================================================================================
PRINT 'Silver層集計中間テーブルの作成が完了しました。';
PRINT '- agg_inventory_monthly: 月次在庫集計';
PRINT '- agg_procurement_performance: 調達パフォーマンス集計';
PRINT '';
PRINT 'Silver層の全テーブル作成が完了しました。';
PRINT '次はGold層のKPIテーブルを作成してください。';
