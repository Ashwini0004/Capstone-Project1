-- Valid customer references in accounts, loans, transactions
-- Just View

CREATE MATERIALIZED VIEW gold.customers_valid AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  a.account_id,
  t.transaction_id,
  l.loan_id
FROM silver.customers_scd2 c
JOIN silver.accounts_clean_pl a
  ON c.customer_id = a.customer_id
JOIN silver.transactions_clean_pl t
  ON a.account_id = t.from_account_id
JOIN silver.loans_clean_pl l
  ON a.account_id = l.account_id
  AND c.customer_id = l.customer_id;
  -------------------------------
  --Valid customer references in accounts, loans, transactions

CREATE OR REPLACE MATERIALIZED VIEW gold.customers_accounts_loans AS
SELECT
  c.customer_id,
  a.account_id,
  a.available_balance,
  c.is_active,
  l.loan_id,
  c.risk_category
FROM
  silver.customers_scd2 c
JOIN
  silver.accounts_clean_pl a
  ON c.customer_id = a.customer_id
LEFT JOIN
  silver.loans_clean_pl l
  ON a.account_id = l.account_id
  AND c.customer_id = l.customer_id
WHERE
  c.is_active = TRUE;

  --------------------------------
-- LOAN: default rates, outstanding amounts, interest rate analytics

CREATE MATERIALIZED VIEW silver.loans_risk_matview AS
SELECT *,
  CASE 
    WHEN loan_status = 'default' THEN 1
    ELSE 0
  END AS is_default,
  outstanding_amount * interest_rate AS risk_exposure
FROM silver.loans_clean_pl;

-----------
--Transactions: volumes, failure rate, channel mix avg/median stats

CREATE OR REPLACE MATERIALIZED VIEW silver.transactions_stats_matview AS
SELECT
  channel,
  COUNT(transaction_id) AS volume,
  -- Failure rate = number of failed transactions / total transactions
  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 1.0 / COUNT(transaction_id) AS failure_rate,
  AVG(amount) AS avg_amount,
  approx_percentile(amount, 0.5) AS median_amount
FROM
  silver.transactions_clean_pl
GROUP BY
  channel;

------------------------------

--Active customers/accounts by month

CREATE OR REPLACE MATERIALIZED VIEW gold.active_customers_accounts_by_month_mv AS
WITH active_customers AS (
  SELECT customer_id
  FROM silver.customers_clean_pl
  WHERE is_active = TRUE
),

active_accounts_by_month AS (
  SELECT
    account_id,
    customer_id,
    date_trunc('month', last_transaction_date) AS month
  FROM silver.accounts_clean_pl
  WHERE last_transaction_date IS NOT NULL
)

SELECT
  a.month,
  COUNT(DISTINCT a.customer_id) AS active_customers,
  COUNT(DISTINCT a.account_id) AS active_accounts
FROM
  active_accounts_by_month a
JOIN
  active_customers c
ON
  a.customer_id = c.customer_id
GROUP BY
  a.month
ORDER BY
  a.month;


