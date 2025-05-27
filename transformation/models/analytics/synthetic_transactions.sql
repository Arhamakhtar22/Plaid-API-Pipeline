-- Create a table with synthetic data while preserving Plaid data
CREATE OR REPLACE TABLE synthetic_transactions AS

-- Start with your existing transactions from Plaid (without DATA_SOURCE since it doesn't exist)
SELECT
    TRANSACTION_ID,
    ACCOUNT_ID,
    TRANSACTION_TYPE,
    TRANSACTION_DATE,
    TRANSACTION_DESCRIPTION,
    TRANSACTION_AMOUNT,
    SPENDING_CATEGORY,
    EXTRACTED_AT,
    'plaid' as DATA_SOURCE  -- Add this as a new column
FROM int_transaction_categories

UNION ALL

SELECT
    'SYNTH-INC-' || TO_VARCHAR(ROW_NUMBER() OVER (ORDER BY seq4())) as TRANSACTION_ID,
    'ACCT001' as ACCOUNT_ID,
    'income' as TRANSACTION_TYPE,
    DATEADD('day', 15 + (MOD(seq4(), 6) * 30), CURRENT_DATE() - INTERVAL '180 days') as TRANSACTION_DATE,
    'Paycheck' as TRANSACTION_DESCRIPTION,
    UNIFORM(3500, 4500, RANDOM()) as TRANSACTION_AMOUNT,
    'Income' as SPENDING_CATEGORY,
    CURRENT_TIMESTAMP() as EXTRACTED_AT,
    'synthetic' as DATA_SOURCE
FROM TABLE(GENERATOR(ROWCOUNT => 6))

UNION ALL

-- Add additional categories for more diversity
SELECT
    'SYNTH-ADD-' || TO_VARCHAR(ROW_NUMBER() OVER (ORDER BY seq4())) as TRANSACTION_ID,
    'ACCT001' as ACCOUNT_ID,
    'expense' as TRANSACTION_TYPE,
    DATEADD('day', MOD(seq4(), 180), CURRENT_DATE() - INTERVAL '180 days') as TRANSACTION_DATE,
    CASE
        WHEN MOD(seq4(), 5) = 0 THEN 'Doctor Visit'
        WHEN MOD(seq4(), 5) = 1 THEN 'Streaming Service'
        WHEN MOD(seq4(), 5) = 2 THEN 'Clothing Purchase'
        WHEN MOD(seq4(), 5) = 3 THEN 'Electronics'
        ELSE 'Gym Membership'
        END as TRANSACTION_DESCRIPTION,
    CASE
        WHEN MOD(seq4(), 5) = 0 THEN UNIFORM(75, 200, RANDOM())   -- Healthcare
        WHEN MOD(seq4(), 5) = 1 THEN UNIFORM(10, 20, RANDOM())    -- Subscriptions
        WHEN MOD(seq4(), 5) = 2 THEN UNIFORM(35, 150, RANDOM())   -- Shopping
        WHEN MOD(seq4(), 5) = 3 THEN UNIFORM(50, 1000, RANDOM())  -- Shopping (big)
        ELSE UNIFORM(40, 80, RANDOM())                            -- Fitness
        END as TRANSACTION_AMOUNT,
    CASE
        WHEN MOD(seq4(), 5) = 0 THEN 'Healthcare'
        WHEN MOD(seq4(), 5) = 1 THEN 'Subscriptions'
        WHEN MOD(seq4(), 5) = 2 THEN 'Shopping'
        WHEN MOD(seq4(), 5) = 3 THEN 'Shopping'
        ELSE 'Fitness'
        END as SPENDING_CATEGORY,
    CURRENT_TIMESTAMP() as EXTRACTED_AT,
    'synthetic' as DATA_SOURCE
FROM TABLE(GENERATOR(ROWCOUNT => 300));