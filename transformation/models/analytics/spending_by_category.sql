-- models/analytics/spending_by_category.sql
-- This model aggregates spending by category for analysis
USE DATABASE ARHAM_TEST_DB;
USE SCHEMA ARHAM_TEST_SCHEMA;
CREATE OR REPLACE TABLE spending_by_category AS

WITH categorized_transactions AS (
    SELECT * FROM int_transaction_categories
    WHERE TRANSACTION_TYPE = 'expense'
),

-- Make sure the date handling is explicit and correct
     monthly_spending AS (
         SELECT
             -- Explicitly handle dates - convert to DATE type
             CAST(DATE_TRUNC('MONTH', TRANSACTION_DATE) AS DATE) as MONTH,
             SPENDING_CATEGORY,
             SUM(TRANSACTION_AMOUNT) as MONTHLY_AMOUNT,
             COUNT(DISTINCT TRANSACTION_DATE) as ACTIVE_DAYS,
             AVG(TRANSACTION_AMOUNT) as AVG_DAILY_SPEND,
             MAX(TRANSACTION_AMOUNT) as MAX_DAILY_SPEND
         FROM categorized_transactions
         GROUP BY 1, 2
     )

SELECT
    MONTH,
    SPENDING_CATEGORY,
    MONTHLY_AMOUNT,
    ACTIVE_DAYS,
    AVG_DAILY_SPEND,
    MAX_DAILY_SPEND
FROM monthly_spending
WHERE MONTH IS NOT NULL -- Add an explicit check for NULL dates
ORDER BY MONTH DESC, MONTHLY_AMOUNT DESC;