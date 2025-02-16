prepare_data_query = '''
    DECLARE dates STRING;
    SET dates =  ( SELECT 
        CONCAT("('",STRING_AGG(REPLACE(CAST( date AS STRING), "-", "_"), "', '"),"')") FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY), CURRENT_DATE())) as date);

    EXECUTE IMMEDIATE FORMAT("""
WITH ga_sessions AS 
    (
      SELECT 
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , SUM(ga_sessions) as value 
        FROM `ap_test_data.ga_data`
        WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
        GROUP BY date, dimension
    )
    , ga_page_views AS 
    (
      SELECT 
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , SUM(ga_page_views) as value 
        FROM `ap_test_data.ga_data`
        WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
        GROUP BY date, dimension
    )
    , ga_data_pv_day_before AS (
        SELECT
        event_date AS event_date
        , operating_system as operating_system
        , ga_page_views AS ga_page_views
        , LAG(ga_page_views, 1) OVER (PARTITION BY operating_system ORDER BY event_date ASC) AS ga_page_views_day_before 
        FROM `ap_test_data.ga_data`
        WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)

        )
    , ga_page_views_change AS 
    (
      SELECT
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , ROUND(SAFE_DIVIDE(SUM(ga_page_views) , SUM(ga_page_views_day_before)) - 1.00, 2) as value 
        FROM ga_data_pv_day_before
        GROUP BY date, dimension
    )
    , ga_sales AS 
    (
      SELECT 
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , SUM(ga_sales) as value 
        FROM `ap_test_data.ga_data`
        WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
        GROUP BY event_date, dimension
    )
    , ga_conversion_rate AS 
    (
      SELECT 
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , ROUND(SAFE_DIVIDE(SUM(ga_sales), SUM(ga_sessions)),3) as value 
        FROM `ap_test_data.ga_data`
        WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
        GROUP BY event_date, dimension
    )
    , ga_shop_sales_deviation AS 
    (
      SELECT 
        REPLACE(CAST(DATE(event_date) AS STRING), "-", "_") as date
        , operating_system as dimension
        , ROUND(COALESCE(SUM(ga_sales),0) - COALESCE(SUM(shop_sales),0), 0) as value 
        FROM `ap_test_data.ga_data` AS ga_data
        FULL OUTER JOIN `ap_test_data.shop_data` as shop_data
        USING (event_date, operating_system)
        WHERE DATE(ga_data.event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
        GROUP BY event_date, dimension
    )
    , pivoted AS (
    SELECT 
     "ga_data" AS table
      , "sessions" as kpi
      , * FROM ga_sessions
    PIVOT(
    SUM(value) as value
    FOR date in %s
    )
    
    UNION ALL
    
    SELECT 
     "ga_data" AS table
      , "p_views" as kpi
      , * FROM ga_page_views
    PIVOT(
    SUM(value) as value
    FOR date in %s
    )

    UNION ALL
    
    SELECT 
     "ga_data" AS table
      , "p_views Δ" as kpi  -- Avoid pct sign here, it causes the FORMAT function to be unable to parse the query
      , * FROM ga_page_views_change
    PIVOT(
    SUM(value) as value
    FOR date in %s
    )
    
    UNION ALL
    
    SELECT 
     "ga_data" AS table
      , "sales" as kpi
      , * FROM ga_sales
    PIVOT(
    SUM(value) as value
    FOR date in %s
    )

    UNION ALL
    
    SELECT 
     "webs./shop" AS table
      , "sales Δ" as kpi
      , * FROM ga_shop_sales_deviation
    PIVOT(
    SUM(value) as value
    FOR date in %s
    )

    )
    SELECT dimension, table, kpi, * EXCEPT(dimension, table, kpi)
    FROM pivoted
    ORDER BY LOWER(dimension), kpi, table
    ;
    """, dates, dates, dates, dates, dates);
'''