-- ============================================================================
-- DATABASE DEMONSTRATION: Product Inventory & Orders System
-- PostgreSQL Edition
-- ============================================================================
-- This demonstration covers:
--   1. Schema design with proper relationships
--   2. Progressive SQL queries (simple to complex)
--   3. Different JOIN types and when to use them
--   4. Aggregations and GROUP BY
--   5. Query efficiency analysis
--   6. Query planner usage (EXPLAIN / EXPLAIN ANALYZE)
-- ============================================================================

-- ============================================================================
-- PART 1: SCHEMA CREATION
-- ============================================================================

-- Drop tables if they exist (for clean re-runs)
-- Using CASCADE to handle foreign key dependencies
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS manufacturers CASCADE;

-- -----------------------------------------------------------------------------
-- Table 1: manufacturers
-- Stores information about product manufacturers
-- -----------------------------------------------------------------------------
CREATE TABLE manufacturers (
    manufacturer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    founded_year INTEGER,
    website VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Table 2: categories
-- Product categories for organization
-- -----------------------------------------------------------------------------
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    parent_category_id INTEGER REFERENCES categories(category_id)
);

-- -----------------------------------------------------------------------------
-- Table 3: products
-- Core product information with foreign keys to manufacturers and categories
-- -----------------------------------------------------------------------------
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    quantity_in_stock INTEGER NOT NULL DEFAULT 0 CHECK (quantity_in_stock >= 0),
    manufacturer_id INTEGER REFERENCES manufacturers(manufacturer_id),
    category_id INTEGER REFERENCES categories(category_id),
    weight_kg DECIMAL(8, 3),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Table 4: orders
-- Customer orders header information
-- -----------------------------------------------------------------------------
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    shipping_address TEXT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    total_amount DECIMAL(12, 2),
    notes TEXT
);

-- -----------------------------------------------------------------------------
-- Table 5: order_items
-- Line items for each order (junction table between orders and products)
-- This implements a many-to-many relationship
-- -----------------------------------------------------------------------------
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,  -- Price at time of order (may differ from current)
    discount_percent DECIMAL(5, 2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    UNIQUE (order_id, product_id)  -- Prevent duplicate products in same order
);


-- ============================================================================
-- PART 2: SAMPLE DATA
-- ============================================================================

-- Insert manufacturers
INSERT INTO manufacturers (name, country, founded_year, website) VALUES
    ('TechCorp Industries', 'USA', 1985, 'https://techcorp.example.com'),
    ('EuroElectronics GmbH', 'Germany', 1992, 'https://euroelec.example.de'),
    ('AsiaManufacturing Ltd', 'Japan', 1978, 'https://asiamfg.example.jp'),
    ('Nordic Components', 'Sweden', 2001, 'https://nordic.example.se'),
    ('Pacific Goods Co', 'Australia', 2010, NULL);  -- Note: NULL website

-- Insert categories (including hierarchical structure)
INSERT INTO categories (name, description, parent_category_id) VALUES
    ('Electronics', 'Electronic devices and components', NULL),
    ('Computers', 'Desktop and laptop computers', 1),
    ('Peripherals', 'Computer peripherals and accessories', 1),
    ('Audio', 'Audio equipment and accessories', 1),
    ('Home & Garden', 'Home and garden products', NULL),
    ('Furniture', 'Home and office furniture', 5),
    ('Discontinued', 'Products no longer sold', NULL);  -- Category with no products

-- Insert products
INSERT INTO products (name, sku, description, price, quantity_in_stock, manufacturer_id, category_id, weight_kg, is_active) VALUES
    ('Pro Laptop 15"', 'TECH-LAP-001', 'High-performance laptop with 15" display', 1299.99, 45, 1, 2, 2.1, TRUE),
    ('Wireless Mouse', 'TECH-MOU-001', 'Ergonomic wireless mouse', 49.99, 200, 1, 3, 0.12, TRUE),
    ('Mechanical Keyboard', 'EURO-KEY-001', 'RGB mechanical keyboard with Cherry MX switches', 159.99, 75, 2, 3, 0.95, TRUE),
    ('Studio Headphones', 'ASIA-AUD-001', 'Professional studio monitoring headphones', 299.99, 30, 3, 4, 0.35, TRUE),
    ('USB-C Hub', 'ASIA-HUB-001', '7-port USB-C hub with power delivery', 79.99, 150, 3, 3, 0.18, TRUE),
    ('Standing Desk', 'NORD-DSK-001', 'Electric height-adjustable standing desk', 599.99, 20, 4, 6, 35.0, TRUE),
    ('Monitor Arm', 'NORD-ARM-001', 'Dual monitor arm mount', 129.99, 60, 4, 3, 3.5, TRUE),
    ('Webcam 4K', 'TECH-CAM-001', '4K webcam with autofocus', 199.99, 85, 1, 3, 0.15, TRUE),
    ('Bluetooth Speaker', 'PACI-SPK-001', 'Portable Bluetooth speaker', 89.99, 0, 5, 4, 0.45, TRUE),  -- Out of stock
    ('Legacy Printer', 'EURO-PRT-001', 'Discontinued thermal printer', 199.99, 5, 2, 3, 4.2, FALSE),  -- Inactive product
    ('Budget Mouse', 'PACI-MOU-001', 'Basic wired mouse', 9.99, 500, 5, 3, 0.08, TRUE),
    ('Orphan Product', 'ORPH-001', 'Product with no manufacturer', 19.99, 10, NULL, 3, 0.1, TRUE);  -- NULL manufacturer

-- Insert orders
INSERT INTO orders (customer_name, customer_email, shipping_address, order_date, status, total_amount, notes) VALUES
    ('Alice Johnson', 'alice@example.com', '123 Main St, New York, NY 10001', '2024-01-15 10:30:00', 'delivered', 1509.97, NULL),
    ('Bob Smith', 'bob@example.com', '456 Oak Ave, Los Angeles, CA 90001', '2024-01-20 14:45:00', 'shipped', 389.97, 'Gift wrap requested'),
    ('Carol White', 'carol@example.com', '789 Pine Rd, Chicago, IL 60601', '2024-02-01 09:15:00', 'processing', 599.99, NULL),
    ('David Brown', 'david@example.com', '321 Elm St, Houston, TX 77001', '2024-02-10 16:00:00', 'pending', 159.98, 'Expedited shipping'),
    ('Eve Davis', 'eve@example.com', '654 Maple Dr, Phoenix, AZ 85001', '2024-02-15 11:30:00', 'cancelled', 299.99, 'Customer changed mind'),
    ('Frank Miller', 'frank@example.com', '987 Cedar Ln, Seattle, WA 98101', '2024-02-20 13:00:00', 'delivered', 2079.96, NULL),
    ('Grace Lee', 'grace@example.com', '147 Birch Way, Boston, MA 02101', '2024-03-01 10:00:00', 'shipped', 49.99, NULL);

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent) VALUES
    -- Order 1: Alice - Laptop + Mouse + Keyboard
    (1, 1, 1, 1299.99, 0),
    (1, 2, 1, 49.99, 0),
    (1, 3, 1, 159.99, 0),
    -- Order 2: Bob - Headphones + USB Hub
    (2, 4, 1, 299.99, 0),
    (2, 5, 1, 79.99, 10),  -- 10% discount
    -- Order 3: Carol - Standing Desk
    (3, 6, 1, 599.99, 0),
    -- Order 4: David - Keyboard + Budget Mouse
    (4, 3, 1, 159.99, 0),
    (4, 11, 1, 9.99, 0),
    -- Order 5: Eve - Headphones (cancelled order)
    (5, 4, 1, 299.99, 0),
    -- Order 6: Frank - Laptop + Webcam + Monitor Arm (x2)
    (6, 1, 1, 1299.99, 5),   -- 5% discount
    (6, 8, 1, 199.99, 0),
    (6, 7, 2, 129.99, 0),
    -- Order 7: Grace - Mouse only
    (7, 2, 1, 49.99, 0);


-- ============================================================================
-- PART 3: SIMPLE QUERIES (Single Table)
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Basic SELECT - Retrieve all columns from a table
-- -----------------------------------------------------------------------------
SELECT * FROM products;

-- -----------------------------------------------------------------------------
-- 3.2 SELECT with specific columns - Better practice than SELECT *
-- Explicitly naming columns improves:
--   - Performance (only fetch needed data)
--   - Clarity (readers know exactly what data is used)
--   - Stability (won't break if columns are added/removed)
-- -----------------------------------------------------------------------------
SELECT product_id, name, price, quantity_in_stock
FROM products;

-- -----------------------------------------------------------------------------
-- 3.3 WHERE clause - Filtering rows
-- -----------------------------------------------------------------------------
-- Find products under $100
SELECT name, price
FROM products
WHERE price < 100;

-- Find products that are in stock AND active
SELECT name, price, quantity_in_stock
FROM products
WHERE quantity_in_stock > 0
  AND is_active = TRUE;

-- Find products from a specific manufacturer
SELECT name, price
FROM products
WHERE manufacturer_id = 1;

-- -----------------------------------------------------------------------------
-- 3.4 ORDER BY - Sorting results
-- -----------------------------------------------------------------------------
-- Sort products by price (ascending is default)
SELECT name, price
FROM products
ORDER BY price;

-- Sort by price descending, then by name ascending
SELECT name, price, quantity_in_stock
FROM products
ORDER BY price DESC, name ASC;

-- -----------------------------------------------------------------------------
-- 3.5 LIMIT and OFFSET - Pagination
-- -----------------------------------------------------------------------------
-- Get the 5 most expensive products
SELECT name, price
FROM products
ORDER BY price DESC
LIMIT 5;

-- Skip first 5, get next 5 (page 2)
SELECT name, price
FROM products
ORDER BY price DESC
LIMIT 5 OFFSET 5;

-- -----------------------------------------------------------------------------
-- 3.6 DISTINCT - Remove duplicates
-- -----------------------------------------------------------------------------
-- Get unique countries where manufacturers are based
SELECT DISTINCT country
FROM manufacturers;

-- -----------------------------------------------------------------------------
-- 3.7 NULL handling
-- Important: NULL requires special treatment with IS NULL / IS NOT NULL
-- Using = NULL will NOT work as expected!
-- -----------------------------------------------------------------------------
-- Find manufacturers without a website
SELECT name, country
FROM manufacturers
WHERE website IS NULL;

-- Find products without a manufacturer (orphaned products)
SELECT name, sku
FROM products
WHERE manufacturer_id IS NULL;

-- COALESCE: Provide default value for NULL
SELECT name, COALESCE(website, 'No website listed') AS website
FROM manufacturers;

-- -----------------------------------------------------------------------------
-- 3.8 String operations and LIKE
-- -----------------------------------------------------------------------------
-- Find products with 'USB' in the name
SELECT name, price
FROM products
WHERE name LIKE '%USB%';

-- Find SKUs starting with 'TECH'
SELECT name, sku
FROM products
WHERE sku LIKE 'TECH%';

-- Case-insensitive search using ILIKE (PostgreSQL specific)
SELECT name
FROM products
WHERE name ILIKE '%mouse%';

-- Alternative: Using LOWER() for case-insensitive search
SELECT name
FROM products
WHERE LOWER(name) LIKE '%mouse%';


-- ============================================================================
-- PART 4: JOINS - Combining Multiple Tables
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 INNER JOIN - Returns only matching rows from both tables
-- This is the most common join type
-- -----------------------------------------------------------------------------
-- Get products with their manufacturer names
SELECT
    p.name AS product_name,
    p.price,
    m.name AS manufacturer_name,
    m.country
FROM products p
INNER JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id;

-- Note: The 'Orphan Product' (manufacturer_id = NULL) is NOT included
-- because INNER JOIN requires a match in BOTH tables

-- -----------------------------------------------------------------------------
-- 4.2 LEFT JOIN (LEFT OUTER JOIN) - All rows from left table, matching from right
-- Use when you want ALL records from the primary table, even without matches
-- -----------------------------------------------------------------------------
-- Get ALL products, including those without manufacturers
SELECT
    p.name AS product_name,
    p.price,
    COALESCE(m.name, '** No Manufacturer **') AS manufacturer_name
FROM products p
LEFT JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id;

-- Now 'Orphan Product' IS included with NULL manufacturer info

-- Find products that DON'T have a manufacturer (using LEFT JOIN + NULL check)
SELECT
    p.name AS product_name,
    p.sku
FROM products p
LEFT JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.manufacturer_id IS NULL;

-- -----------------------------------------------------------------------------
-- 4.3 RIGHT JOIN - All rows from right table, matching from left
-- PostgreSQL fully supports RIGHT JOIN
-- Conceptually: RIGHT JOIN is just LEFT JOIN with tables swapped
-- -----------------------------------------------------------------------------
-- Get all manufacturers, even those without products
SELECT
    m.name AS manufacturer_name,
    m.country,
    COUNT(p.product_id) AS product_count
FROM products p
RIGHT JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
GROUP BY m.manufacturer_id, m.name, m.country;

-- Equivalent using LEFT JOIN with tables reversed:
SELECT
    m.name AS manufacturer_name,
    m.country,
    COUNT(p.product_id) AS product_count
FROM manufacturers m
LEFT JOIN products p ON p.manufacturer_id = m.manufacturer_id
GROUP BY m.manufacturer_id, m.name, m.country;

-- -----------------------------------------------------------------------------
-- 4.4 FULL OUTER JOIN - All rows from both tables
-- Shows all products (even without manufacturers) AND all manufacturers (even without products)
-- PostgreSQL natively supports FULL OUTER JOIN
-- -----------------------------------------------------------------------------
SELECT
    p.name AS product_name,
    m.name AS manufacturer_name
FROM products p
FULL OUTER JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id;

-- This shows:
-- - Products WITH manufacturers (matched)
-- - Products WITHOUT manufacturers (Orphan Product)
-- - Manufacturers WITHOUT products (if any existed)

-- -----------------------------------------------------------------------------
-- 4.5 Multiple JOINs - Chaining joins across several tables
-- -----------------------------------------------------------------------------
-- Get complete order details with product and manufacturer info
SELECT
    o.order_id,
    o.customer_name,
    o.order_date,
    p.name AS product_name,
    m.name AS manufacturer_name,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS line_total
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
LEFT JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
ORDER BY o.order_id, p.name;

-- -----------------------------------------------------------------------------
-- 4.6 Self JOIN - Joining a table to itself
-- Useful for hierarchical data (like category parent-child relationships)
-- -----------------------------------------------------------------------------
-- Get categories with their parent category names
SELECT
    c.name AS category_name,
    COALESCE(parent.name, '** Top Level **') AS parent_category
FROM categories c
LEFT JOIN categories parent ON c.parent_category_id = parent.category_id;


-- ============================================================================
-- PART 5: AGGREGATIONS
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 5.1 Basic aggregate functions
-- -----------------------------------------------------------------------------
-- COUNT: Number of rows
SELECT COUNT(*) AS total_products FROM products;
SELECT COUNT(manufacturer_id) AS products_with_manufacturer FROM products;  -- Excludes NULLs

-- SUM: Total of values
SELECT SUM(quantity_in_stock) AS total_inventory FROM products;

-- AVG: Average value
SELECT AVG(price) AS average_price FROM products;

-- MIN/MAX: Extremes
SELECT
    MIN(price) AS cheapest,
    MAX(price) AS most_expensive
FROM products;

-- -----------------------------------------------------------------------------
-- 5.2 GROUP BY - Aggregate by categories
-- -----------------------------------------------------------------------------
-- Count products per manufacturer
SELECT
    m.name AS manufacturer_name,
    COUNT(p.product_id) AS product_count,
    AVG(p.price) AS avg_price
FROM manufacturers m
LEFT JOIN products p ON m.manufacturer_id = p.manufacturer_id
GROUP BY m.manufacturer_id, m.name;

-- Total inventory value per category
SELECT
    c.name AS category_name,
    COUNT(p.product_id) AS num_products,
    SUM(p.price * p.quantity_in_stock) AS inventory_value
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_id, c.name
ORDER BY inventory_value DESC NULLS LAST;

-- -----------------------------------------------------------------------------
-- 5.3 HAVING - Filter after aggregation
-- WHERE filters rows BEFORE grouping, HAVING filters AFTER grouping
-- -----------------------------------------------------------------------------
-- Find manufacturers with more than 2 products
SELECT
    m.name AS manufacturer_name,
    COUNT(p.product_id) AS product_count
FROM manufacturers m
JOIN products p ON m.manufacturer_id = p.manufacturer_id
GROUP BY m.manufacturer_id, m.name
HAVING COUNT(p.product_id) > 2;

-- Find categories with average price over $100
SELECT
    c.name AS category_name,
    AVG(p.price) AS avg_price
FROM categories c
JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_id, c.name
HAVING AVG(p.price) > 100;

-- -----------------------------------------------------------------------------
-- 5.4 Complex aggregation example: Sales analysis
-- -----------------------------------------------------------------------------
-- Revenue by customer (excluding cancelled orders)
SELECT
    o.customer_name,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(oi.quantity) AS total_items,
    SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status != 'cancelled'
GROUP BY o.customer_name
ORDER BY total_revenue DESC;

-- Best-selling products
SELECT
    p.name AS product_name,
    SUM(oi.quantity) AS total_sold,
    SUM(oi.quantity * oi.unit_price) AS gross_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status != 'cancelled'
GROUP BY p.product_id, p.name
ORDER BY total_sold DESC;


-- ============================================================================
-- PART 6: EFFICIENT vs INEFFICIENT QUERIES
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 6.1 Using indexes effectively
-- First, let's create some indexes
-- -----------------------------------------------------------------------------
CREATE INDEX idx_products_manufacturer ON products(manufacturer_id);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_date ON orders(order_date);

-- Analyze tables to update statistics (important for query planner!)
ANALYZE manufacturers;
ANALYZE categories;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;

-- -----------------------------------------------------------------------------
-- 6.2 EFFICIENT: Using indexed columns in WHERE clause
-- -----------------------------------------------------------------------------
-- GOOD: Uses the index on manufacturer_id
SELECT name, price
FROM products
WHERE manufacturer_id = 1;

-- -----------------------------------------------------------------------------
-- 6.3 INEFFICIENT: Function on indexed column prevents index use
-- -----------------------------------------------------------------------------
-- BAD: EXTRACT() function prevents using index on order_date
-- The database must scan every row and compute EXTRACT() for each
SELECT *
FROM orders
WHERE EXTRACT(YEAR FROM order_date) = 2024;

-- GOOD: Use range comparison instead - can use the index!
SELECT *
FROM orders
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01';

-- -----------------------------------------------------------------------------
-- 6.4 INEFFICIENT: Using OR with different columns
-- -----------------------------------------------------------------------------
-- BAD: OR across different columns often can't use indexes efficiently
-- May result in full table scan or inefficient bitmap OR
SELECT *
FROM products
WHERE manufacturer_id = 1 OR category_id = 2;

-- BETTER: Use UNION for better index utilization (if needed)
SELECT * FROM products WHERE manufacturer_id = 1
UNION
SELECT * FROM products WHERE category_id = 2;

-- -----------------------------------------------------------------------------
-- 6.5 INEFFICIENT: SELECT * when you only need specific columns
-- -----------------------------------------------------------------------------
-- BAD: Fetches ALL columns even if you only need a few
-- Wastes memory, network bandwidth, and may prevent index-only scan
SELECT *
FROM products p
JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.country = 'USA';

-- GOOD: Only select columns you actually need
-- May enable "index-only scan" if columns are in the index
SELECT p.name, p.price, m.name AS manufacturer
FROM products p
JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.country = 'USA';

-- -----------------------------------------------------------------------------
-- 6.6 INEFFICIENT: Not using EXISTS for existence checks
-- -----------------------------------------------------------------------------
-- BAD: IN with subquery - subquery may execute fully before comparison
-- For large datasets, this materializes the entire subquery result
SELECT *
FROM manufacturers
WHERE manufacturer_id IN (
    SELECT DISTINCT manufacturer_id
    FROM products
    WHERE price > 500
);

-- GOOD: EXISTS stops as soon as it finds a match
-- More efficient for "does any row exist?" checks
SELECT *
FROM manufacturers m
WHERE EXISTS (
    SELECT 1
    FROM products p
    WHERE p.manufacturer_id = m.manufacturer_id
      AND p.price > 500
);

-- -----------------------------------------------------------------------------
-- 6.7 INEFFICIENT: Correlated subquery in SELECT (N+1 problem)
-- -----------------------------------------------------------------------------
-- BAD: This subquery runs once PER ROW in the outer query!
-- For 1000 products, this runs 1000 subqueries
SELECT
    p.name,
    p.price,
    (SELECT COUNT(*)
     FROM order_items oi
     WHERE oi.product_id = p.product_id) AS times_ordered
FROM products p;

-- GOOD: Use a single JOIN with aggregation
SELECT
    p.name,
    p.price,
    COALESCE(oi_agg.order_count, 0) AS times_ordered
FROM products p
LEFT JOIN (
    SELECT product_id, COUNT(*) AS order_count
    FROM order_items
    GROUP BY product_id
) oi_agg ON p.product_id = oi_agg.product_id;

-- -----------------------------------------------------------------------------
-- 6.8 INEFFICIENT: LIKE with leading wildcard
-- -----------------------------------------------------------------------------
-- BAD: Leading wildcard '%keyboard' cannot use index
-- Must scan all rows to find matches
SELECT name, price
FROM products
WHERE name LIKE '%Keyboard%';

-- GOOD: Leading literal can use index (if index exists on name)
SELECT name, price
FROM products
WHERE name LIKE 'Mechanical%';

-- NOTE: For full-text search needs, use PostgreSQL's powerful FTS features:
-- CREATE INDEX idx_products_name_fts ON products USING gin(to_tsvector('english', name));
-- SELECT * FROM products WHERE to_tsvector('english', name) @@ to_tsquery('keyboard');

-- -----------------------------------------------------------------------------
-- 6.9 INEFFICIENT: Unnecessary DISTINCT
-- -----------------------------------------------------------------------------
-- BAD: DISTINCT requires sorting/hashing all results
-- Don't use it just "to be safe" - understand your data
SELECT DISTINCT m.name
FROM manufacturers m
JOIN products p ON m.manufacturer_id = p.manufacturer_id;

-- GOOD: If you know the join won't produce duplicates, don't use DISTINCT
-- Or restructure the query to avoid duplicates in the first place
SELECT m.name
FROM manufacturers m
WHERE EXISTS (SELECT 1 FROM products p WHERE p.manufacturer_id = m.manufacturer_id);

-- -----------------------------------------------------------------------------
-- 6.10 INEFFICIENT: Ordering before necessary
-- -----------------------------------------------------------------------------
-- BAD: Sorting a large intermediate result before filtering it down
SELECT *
FROM (
    SELECT p.*, m.name AS manufacturer_name
    FROM products p
    JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
    ORDER BY p.price DESC  -- Sorting ALL rows
) subq
WHERE manufacturer_name = 'TechCorp Industries'
LIMIT 5;

-- GOOD: Filter first, then sort only the relevant rows
SELECT p.*, m.name AS manufacturer_name
FROM products p
JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.name = 'TechCorp Industries'
ORDER BY p.price DESC
LIMIT 5;


-- ============================================================================
-- PART 7: QUERY PLANNER / EXPLAIN
-- ============================================================================
-- The query planner (optimizer) decides HOW to execute your query.
-- EXPLAIN shows you the execution plan - invaluable for optimization!

-- -----------------------------------------------------------------------------
-- 7.1 Basic EXPLAIN usage
-- -----------------------------------------------------------------------------
-- Shows the planned execution steps (does NOT execute the query)
EXPLAIN
SELECT p.name, m.name AS manufacturer
FROM products p
JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.country = 'USA';

-- -----------------------------------------------------------------------------
-- 7.2 EXPLAIN ANALYZE - Actually runs the query and shows real timing
-- This is the most useful form for performance analysis
-- -----------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT p.name, m.name AS manufacturer
FROM products p
JOIN manufacturers m ON p.manufacturer_id = m.manufacturer_id
WHERE m.country = 'USA';

-- -----------------------------------------------------------------------------
-- 7.3 Understanding EXPLAIN output
-- -----------------------------------------------------------------------------
-- Key things to look for:
--
-- 1. Seq Scan vs Index Scan
--    - Seq Scan = sequential scan, reading all rows - often bad for large tables
--    - Index Scan = using an index - usually good
--    - Index Only Scan = all needed data is in the index (best!)
--    - Bitmap Index Scan = using index to build a bitmap, then fetch rows
--
-- 2. Cost estimates: (startup_cost..total_cost)
--    - Startup cost: time before first row returned
--    - Total cost: time to return all rows
--    - Lower is better, but these are estimates!
--
-- 3. Rows: estimated number of rows returned
--    - Compare with "actual rows" in EXPLAIN ANALYZE
--    - Large discrepancy suggests stale statistics (run ANALYZE)
--
-- 4. Width: estimated average row size in bytes
--
-- 5. Join types:
--    - Nested Loop: good for small tables or indexed lookups
--    - Hash Join: good for larger tables without useful indexes
--    - Merge Join: good when both inputs are sorted

-- Example: See the difference with and without an index
EXPLAIN ANALYZE
SELECT * FROM products WHERE price > 500;

-- vs (this should use the index)
EXPLAIN ANALYZE
SELECT * FROM products WHERE manufacturer_id = 1;

-- -----------------------------------------------------------------------------
-- 7.4 EXPLAIN with additional options
-- -----------------------------------------------------------------------------
-- BUFFERS: Shows buffer usage (cache hits vs disk reads)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM products WHERE manufacturer_id = 1;

-- FORMAT options: TEXT (default), JSON, YAML, XML
-- JSON is useful for programmatic analysis
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT * FROM products WHERE manufacturer_id = 1;

-- VERBOSE: Shows additional details like output columns
EXPLAIN (ANALYZE, VERBOSE)
SELECT p.name, p.price
FROM products p
WHERE p.manufacturer_id = 1;

-- -----------------------------------------------------------------------------
-- 7.5 EXPLAIN for JOIN analysis
-- -----------------------------------------------------------------------------
-- See which table is scanned vs indexed, and join order
EXPLAIN ANALYZE
SELECT
    o.order_id,
    o.customer_name,
    p.name,
    oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'delivered';

-- -----------------------------------------------------------------------------
-- 7.6 Using EXPLAIN to compare query alternatives
-- -----------------------------------------------------------------------------
-- Compare the two approaches from section 6.6:

-- IN subquery approach:
EXPLAIN ANALYZE
SELECT *
FROM manufacturers
WHERE manufacturer_id IN (
    SELECT DISTINCT manufacturer_id FROM products WHERE price > 500
);

-- EXISTS approach:
EXPLAIN ANALYZE
SELECT *
FROM manufacturers m
WHERE EXISTS (
    SELECT 1 FROM products p
    WHERE p.manufacturer_id = m.manufacturer_id AND p.price > 500
);

-- -----------------------------------------------------------------------------
-- 7.7 Interpreting common plan nodes
-- -----------------------------------------------------------------------------
-- Here's a quick reference for common PostgreSQL plan nodes:
--
-- SCANS:
--   Seq Scan         - Full table scan (reads every row)
--   Index Scan       - Uses index to find rows, then fetches from table
--   Index Only Scan  - All data comes from index (no table access!)
--   Bitmap Heap Scan - Uses bitmap from index scan to fetch rows
--
-- JOINS:
--   Nested Loop      - For each outer row, scan inner (good with index)
--   Hash Join        - Build hash table from one side, probe with other
--   Merge Join       - Merge two sorted inputs
--
-- OTHER:
--   Sort             - Sorts input rows (may spill to disk if large)
--   Aggregate        - Computes aggregates (COUNT, SUM, etc.)
--   HashAggregate    - Aggregation using hash table
--   GroupAggregate   - Aggregation on sorted input
--   Limit            - Returns only first N rows
--   Materialize      - Stores results for reuse


-- ============================================================================
-- PART 8: ADVANCED TOPICS
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 8.1 Common Table Expressions (CTEs) - WITH clause
-- Improves readability and allows "temp views" within a query
-- -----------------------------------------------------------------------------
WITH expensive_products AS (
    SELECT product_id, name, price
    FROM products
    WHERE price > 200
),
recent_orders AS (
    SELECT order_id, customer_name, order_date
    FROM orders
    WHERE order_date >= '2024-02-01'
)
SELECT
    ro.customer_name,
    ep.name AS product,
    ep.price
FROM recent_orders ro
JOIN order_items oi ON ro.order_id = oi.order_id
JOIN expensive_products ep ON oi.product_id = ep.product_id;

-- -----------------------------------------------------------------------------
-- 8.2 Window Functions (for running totals, rankings, etc.)
-- -----------------------------------------------------------------------------
-- Rank products by price within each category
SELECT
    c.name AS category,
    p.name AS product,
    p.price,
    RANK() OVER (PARTITION BY p.category_id ORDER BY p.price DESC) AS price_rank
FROM products p
JOIN categories c ON p.category_id = c.category_id
WHERE p.is_active = TRUE;

-- Running total of orders by date
SELECT
    order_date,
    customer_name,
    total_amount,
    SUM(total_amount) OVER (ORDER BY order_date) AS running_total
FROM orders
WHERE status != 'cancelled';

-- ROW_NUMBER, RANK, DENSE_RANK comparison
SELECT
    name,
    price,
    ROW_NUMBER() OVER (ORDER BY price DESC) AS row_num,    -- Always unique
    RANK() OVER (ORDER BY price DESC) AS rank,              -- Gaps after ties
    DENSE_RANK() OVER (ORDER BY price DESC) AS dense_rank   -- No gaps after ties
FROM products;

-- LAG and LEAD: Access previous/next rows
SELECT
    order_date,
    customer_name,
    total_amount,
    LAG(total_amount) OVER (ORDER BY order_date) AS prev_order_amount,
    LEAD(total_amount) OVER (ORDER BY order_date) AS next_order_amount
FROM orders
WHERE status != 'cancelled';

-- -----------------------------------------------------------------------------
-- 8.3 CASE expressions - Conditional logic in queries
-- -----------------------------------------------------------------------------
SELECT
    name,
    price,
    quantity_in_stock,
    CASE
        WHEN quantity_in_stock = 0 THEN 'Out of Stock'
        WHEN quantity_in_stock < 20 THEN 'Low Stock'
        WHEN quantity_in_stock < 100 THEN 'In Stock'
        ELSE 'Well Stocked'
    END AS stock_status
FROM products;

-- Using CASE in aggregations (pivot-like query)
SELECT
    COUNT(CASE WHEN status = 'delivered' THEN 1 END) AS delivered,
    COUNT(CASE WHEN status = 'shipped' THEN 1 END) AS shipped,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled
FROM orders;

-- Using FILTER clause (PostgreSQL-specific, cleaner than CASE)
SELECT
    COUNT(*) FILTER (WHERE status = 'delivered') AS delivered,
    COUNT(*) FILTER (WHERE status = 'shipped') AS shipped,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
FROM orders;

-- -----------------------------------------------------------------------------
-- 8.4 PostgreSQL-specific features
-- -----------------------------------------------------------------------------
-- RETURNING clause: Get data back from INSERT/UPDATE/DELETE
INSERT INTO manufacturers (name, country, founded_year)
VALUES ('NewCorp', 'Canada', 2024)
RETURNING manufacturer_id, name;

-- Delete the test record
DELETE FROM manufacturers WHERE name = 'NewCorp';

-- Array aggregation
SELECT
    m.name AS manufacturer,
    ARRAY_AGG(p.name ORDER BY p.price DESC) AS products
FROM manufacturers m
JOIN products p ON m.manufacturer_id = p.manufacturer_id
GROUP BY m.manufacturer_id, m.name;

-- String aggregation with STRING_AGG
SELECT
    c.name AS category,
    STRING_AGG(p.name, ', ' ORDER BY p.name) AS product_list
FROM categories c
JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_id, c.name;


-- ============================================================================
-- END OF SQL DEMONSTRATION
-- ============================================================================
