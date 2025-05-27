-- creating a CTE that contains all records from the staging table
CREATE OR REPLACE TABLE int_transaction_categories AS
WITH transactions AS (
    SELECT * FROM stg_transactions
),

-- reducing hundreds of raw categories to 12 standardized ones
category_mapping AS (
-- This creates a standardized category mapping based on the primary category
SELECT
    TRANSACTION_ID,
    PRIMARY_CATEGORY,
    CASE
    WHEN PRIMARY_CATEGORY IN ('Food and Drink', 'Restaurants', 'Food', 'Restaurant') THEN 'Dining'
    WHEN PRIMARY_CATEGORY IN ('Travel', 'Airlines and Aviation Services', 'Hotels and Motels', 'Car Service', 'Ride Share', 'Hotel') THEN 'Travel'
    WHEN PRIMARY_CATEGORY IN ('Rent', 'Mortgage', 'Home Improvement', 'Housing', 'Rent or Mortgage') THEN 'Housing'
    WHEN PRIMARY_CATEGORY IN ('Groceries', 'Supermarkets and Groceries', 'Supermarket') THEN 'Groceries'
    WHEN PRIMARY_CATEGORY IN ('Transfer', 'Credit Card', 'Payment', 'Bank Transfer') THEN 'Transfers'
    WHEN PRIMARY_CATEGORY IN ('Shops', 'Shopping', 'Retail', 'Clothing') THEN 'Shopping'
    WHEN PRIMARY_CATEGORY IN ('Recreation', 'Entertainment', 'Arts and Entertainment', 'Recreation and Entertainment') THEN 'Entertainment'
    WHEN PRIMARY_CATEGORY IN ('Deposit', 'Interest', 'Payroll', 'Income', 'Salary') THEN 'Income'
    WHEN PRIMARY_CATEGORY IN ('Healthcare', 'Medical', 'Pharmacy', 'Health and Medical') THEN 'Healthcare'
    WHEN PRIMARY_CATEGORY IN ('Utilities', 'Electric', 'Gas', 'Water', 'Internet', 'Phone', 'Telecommunications') THEN 'Utilities'
    WHEN PRIMARY_CATEGORY IN ('Transportation', 'Public Transportation', 'Taxi', 'Car Service') THEN 'Transportation'
    WHEN PRIMARY_CATEGORY IN ('Education', 'Schools', 'Tuition', 'Student Loans') THEN 'Education'
    ELSE 'Other'
    END as SPENDING_CATEGORY
FROM transactions
    )

-- joining the original transaction data with the standardized category
SELECT
    t.*,
    c.SPENDING_CATEGORY -- this is a new field from the mapping
FROM transactions t
         JOIN category_mapping c ON t.TRANSACTION_ID = c.TRANSACTION_ID