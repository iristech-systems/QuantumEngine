-- ============================================================================
-- ClickHouse Schema for Marketplace Monitoring Data
-- Optimized for price correlation analysis, buybox tracking, and seller analytics
-- ============================================================================

-- Main marketplace data table
CREATE TABLE marketplace_data (
    -- Core identifiers (part of ORDER BY for optimal performance)
    product_sku_model_number String,
    seller_name LowCardinality(String),
    marketplace LowCardinality(String),
    
    -- Time dimension (critical for partitioning and ordering)
    date_collected DateTime64(3, 'UTC'),
    
    -- Pricing data (your main metrics)
    offer_price Decimal(12,2),
    product_msrp Nullable(Decimal(12,2)),
    product_pmap Nullable(Decimal(12,2)),
    product_umap Nullable(Decimal(12,2)),
    percent_difference Nullable(Float64),
    
    -- Boolean flags (stored as UInt8 for performance)
    below_map UInt8 DEFAULT 0,
    above_map UInt8 DEFAULT 0,
    key_product UInt8 DEFAULT 0,
    buybox_winner UInt8 DEFAULT 0,
    reseller_auth UInt8 DEFAULT 0,
    reseller_internet_auth UInt8 DEFAULT 0,
    reseller_online_location_auth UInt8 DEFAULT 0,
    reseller_product_auth UInt8 DEFAULT 0,
    reseller_is_registered UInt8 DEFAULT 0,
    processed_by_portal UInt8 DEFAULT 0,
    does_not_count UInt8 DEFAULT 0,
    
    -- Product categorization
    product_brand LowCardinality(String),
    product_category LowCardinality(String),
    product_categories Array(LowCardinality(String)),
    product_brands Array(LowCardinality(String)),
    product_type LowCardinality(String),
    product_variation LowCardinality(String),
    condition LowCardinality(String),
    listing_type LowCardinality(String),
    offer_type LowCardinality(String),
    
    -- Identifiers and keys
    product_key String,
    reseller_key LowCardinality(String),
    product_ean String,
    product_asin String,
    currency_code LowCardinality(String),
    product_currency_code LowCardinality(String),
    
    -- Text fields (less frequently queried)
    product_name String,
    product_display_name String,
    product_description String,
    product_alt_model String,
    ignore_words String,
    
    -- URLs (stored compressed)
    ad_page_url String CODEC(ZSTD(3)),
    buy_page_url String CODEC(ZSTD(3)),
    
    -- Metadata
    source_file LowCardinality(String),
    
    -- Derived fields for faster queries
    price_tier LowCardinality(String) MATERIALIZED (
        CASE 
            WHEN offer_price < 50 THEN 'budget'
            WHEN offer_price < 200 THEN 'mid'
            ELSE 'premium'
        END
    ),
    
    date_only Date MATERIALIZED toDate(date_collected),
    year_month UInt32 MATERIALIZED toYYYYMM(date_collected),
    
    -- Pre-calculated flags for common filters
    is_map_violation UInt8 MATERIALIZED (below_map OR above_map),
    is_amazon UInt8 MATERIALIZED (seller_name = 'Amazon.com')

) ENGINE = ReplacingMergeTree(date_collected)  -- Handle duplicate data
PARTITION BY toYYYYMM(date_collected)          -- Monthly partitions
ORDER BY (
    seller_name,                               -- Primary sort key
    product_sku_model_number,                  -- Secondary sort key  
    date_collected                             -- Temporal sort
)
SETTINGS 
    index_granularity = 8192,                 -- Default granularity
    merge_max_block_size = 8192,              -- Optimize for your data size
    ttl_only_drop_parts = 1;                  -- Efficient TTL handling

-- ============================================================================
-- Data Skipping Indexes (Critical for Performance)
-- ============================================================================

-- Bloom filter for high-cardinality SKU searches
ALTER TABLE marketplace_data 
ADD INDEX idx_sku_bloom product_sku_model_number 
TYPE bloom_filter(0.01) GRANULARITY 3;

-- Bloom filter for product categories (array searches)
ALTER TABLE marketplace_data 
ADD INDEX idx_categories_bloom product_categories 
TYPE bloom_filter(0.01) GRANULARITY 3;

-- Bloom filter for product brands (array searches)  
ALTER TABLE marketplace_data 
ADD INDEX idx_brands_bloom product_brands 
TYPE bloom_filter(0.01) GRANULARITY 3;

-- Set index for marketplace (low cardinality)
ALTER TABLE marketplace_data 
ADD INDEX idx_marketplace_set marketplace 
TYPE set(100) GRANULARITY 1;

-- Set index for product categories (frequent filters)
ALTER TABLE marketplace_data 
ADD INDEX idx_category_set product_category 
TYPE set(1000) GRANULARITY 1;

-- Set index for product brands (frequent filters)
ALTER TABLE marketplace_data 
ADD INDEX idx_brand_set product_brand 
TYPE set(500) GRANULARITY 1;

-- MinMax index for price range queries
ALTER TABLE marketplace_data 
ADD INDEX idx_price_minmax offer_price 
TYPE minmax GRANULARITY 4;

-- Boolean flag indexes (very efficient for filtering)
ALTER TABLE marketplace_data 
ADD INDEX idx_below_map below_map 
TYPE set(2) GRANULARITY 1;

ALTER TABLE marketplace_data 
ADD INDEX idx_buybox_winner buybox_winner 
TYPE set(2) GRANULARITY 1;

ALTER TABLE marketplace_data 
ADD INDEX idx_key_product key_product 
TYPE set(2) GRANULARITY 1;

-- ============================================================================
-- Materialized Views for Common Aggregations
-- ============================================================================

-- Daily price aggregates (for correlation analysis)
CREATE MATERIALIZED VIEW mv_daily_prices_by_seller
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (seller_name, product_sku_model_number, date)
AS SELECT
    seller_name,
    product_sku_model_number,
    product_brand,
    toDate(date_collected) as date,
    avg(offer_price) as avg_price,
    min(offer_price) as min_price,
    max(offer_price) as max_price,
    count() as price_count,
    any(product_key) as product_key
FROM marketplace_data
GROUP BY seller_name, product_sku_model_number, product_brand, date;

-- Buybox winner statistics (hourly)
CREATE MATERIALIZED VIEW mv_buybox_stats_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (seller_name, marketplace, hour)
AS SELECT
    seller_name,
    marketplace,
    toStartOfHour(date_collected) as hour,
    countIf(buybox_winner = 1) as wins,
    countIf(buybox_winner = 0) as losses,
    count() as total_listings
FROM marketplace_data
WHERE marketplace IN ('Amazon', 'amazon.ca')
GROUP BY seller_name, marketplace, hour;

-- MAP violation tracking
CREATE MATERIALIZED VIEW mv_map_violations_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (product_brand, seller_name, date)
AS SELECT
    product_brand,
    seller_name,
    toDate(date_collected) as date,
    countIf(below_map = 1) as below_map_count,
    countIf(above_map = 1) as above_map_count,
    count() as total_products
FROM marketplace_data
WHERE below_map = 1 OR above_map = 1
GROUP BY product_brand, seller_name, date;

-- ============================================================================
-- TTL (Time To Live) Policies for Data Management
-- ============================================================================

-- Keep raw data for 2 years, aggregated data longer
ALTER TABLE marketplace_data 
MODIFY TTL date_collected + INTERVAL 2 YEAR;

-- Keep daily aggregates for 5 years
ALTER TABLE mv_daily_prices_by_seller 
MODIFY TTL date + INTERVAL 5 YEAR;

-- ============================================================================
-- Projection for Price History Queries (ClickHouse 21.8+)
-- ============================================================================

-- Optimized projection for time-series price queries
ALTER TABLE marketplace_data 
ADD PROJECTION price_history_projection (
    SELECT 
        product_sku_model_number,
        seller_name,
        date_collected,
        offer_price,
        below_map,
        above_map
    ORDER BY product_sku_model_number, seller_name, date_collected
);

-- ============================================================================
-- Helper Table for Product Catalog (Slowly Changing Dimensions)
-- ============================================================================

CREATE TABLE product_catalog (
    product_sku_model_number String,
    product_key String,
    product_name String,
    product_brand LowCardinality(String),
    product_categories Array(LowCardinality(String)),
    product_type LowCardinality(String),
    product_description String,
    current_msrp Nullable(Decimal(12,2)),
    is_active UInt8 DEFAULT 1,
    created_date DateTime DEFAULT now(),
    updated_date DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_date)
ORDER BY product_sku_model_number
SETTINGS index_granularity = 8192;

-- Index for catalog lookups
ALTER TABLE product_catalog 
ADD INDEX idx_catalog_sku_bloom product_sku_model_number 
TYPE bloom_filter(0.01) GRANULARITY 1;

-- ============================================================================
-- Sample Queries Optimized for This Schema
-- ============================================================================

-- 1. Price correlation analysis (your main use case)
/*
WITH reference_prices AS (
    SELECT 
        product_sku_model_number,
        date_only,
        avg(offer_price) as ref_price
    FROM marketplace_data
    WHERE seller_name = 'Amazon.com'
      AND date_collected >= '2024-01-01'
      AND product_sku_model_number IN ('SKU1', 'SKU2', 'SKU3')
    GROUP BY product_sku_model_number, date_only
),
seller_prices AS (
    SELECT 
        product_sku_model_number,
        seller_name,
        date_only,
        avg(offer_price) as seller_price
    FROM marketplace_data
    WHERE seller_name != 'Amazon.com'
      AND date_collected >= '2024-01-01'
      AND product_sku_model_number IN ('SKU1', 'SKU2', 'SKU3')
    GROUP BY product_sku_model_number, seller_name, date_only
)
SELECT 
    sp.product_sku_model_number,
    sp.seller_name,
    corr(rp.ref_price, sp.seller_price) as correlation,
    count() as sample_size
FROM reference_prices rp
JOIN seller_prices sp ON rp.product_sku_model_number = sp.product_sku_model_number 
                     AND rp.date_only = sp.date_only
GROUP BY sp.product_sku_model_number, sp.seller_name
HAVING sample_size >= 5 AND correlation >= 0.7
ORDER BY correlation DESC;
*/

-- 2. Today's buybox winners
/*
SELECT 
    product_sku_model_number,
    seller_name,
    offer_price,
    product_umap,
    below_map,
    date_collected
FROM marketplace_data
WHERE date_only = today()
  AND marketplace IN ('Amazon', 'amazon.ca')
  AND buybox_winner = 1
  AND has(product_categories, 'electronics')
ORDER BY date_collected DESC;
*/

-- 3. MAP violations by brand
/*
SELECT 
    product_brand,
    seller_name,
    count() as violation_count,
    avg(offer_price) as avg_violating_price,
    avg(product_umap) as avg_map_price
FROM marketplace_data
WHERE below_map = 1
  AND date_only >= today() - 7
  AND product_brand != ''
GROUP BY product_brand, seller_name
ORDER BY violation_count DESC;
*/