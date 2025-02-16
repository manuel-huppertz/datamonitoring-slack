
-- Define start and end date parameters
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);
DECLARE end_date DATE DEFAULT CURRENT_DATE();

CREATE SCHEMA IF NOT EXISTS ap_test_data;

-- Generate time series data
CREATE OR REPLACE TABLE ap_test_data.ga_data AS
WITH date_series AS (
  SELECT 
    n as offset,
    DATE_ADD(start_date, INTERVAL n DAY) AS event_date
  FROM 
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
)
SELECT
  event_date AS event_date,
  operating_system AS operating_system,
  ROUND(GREATEST(100 + RAND() * 50, 50)) AS ga_sessions,  -- Simulated sessions
  CASE WHEN RAND() > 0.85 THEN NULL 
  ELSE ROUND(GREATEST(200 + RAND() * 100, 100))
  END AS ga_page_views,  -- Simulated page views with errors
  ROUND(GREATEST(10 + RAND() * 5, 5)) AS ga_sales  -- Simulated sales
FROM date_series
CROSS JOIN UNNEST(['Web', 'iOS', 'Android']) AS operating_system
ORDER BY event_date;

CREATE OR REPLACE TABLE ap_test_data.shop_data AS
-- Generate time series data
WITH date_series AS (
  SELECT 
    n as offset,
    DATE_ADD(start_date, INTERVAL n DAY) AS event_date
  FROM 
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
)
SELECT
  event_date AS event_date,
  operating_system AS operating_system,
  ROUND(GREATEST(10 + RAND() * 5, 5)) AS shop_sales,  -- probably conflicting values with ga registered sales
  ROUND(GREATEST(200 + RAND() * 100, 100)) AS shop_revenue
FROM date_series
CROSS JOIN UNNEST(['Web', 'iOS', 'Android']) AS operating_system
ORDER BY event_date;