-- Data cleansing, deduplication, masking & SCD Type 2

--Data cleansing, deduplication on customers.csv file
CREATE OR REFRESH STREAMING TABLE silver.customers_clean_pl 
( 
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW 
) 
AS 
SELECT DISTINCT * 
FROM stream customers 
WHERE 
  first_name IS NOT NULL AND TRIM(first_name) <> '' AND
  last_name IS NOT NULL AND TRIM(last_name) <> '' AND
  email IS NOT NULL AND TRIM(email) <> '' AND
  phone IS NOT NULL AND TRIM(phone) <> '' AND
  date_of_birth IS NOT NULL AND TRIM(date_of_birth) <> '' AND
  gender IS NOT NULL AND TRIM(gender) <> '' AND
  annual_income IS NOT NULL AND TRIM(annual_income) <> '' AND
  pan_number IS NOT NULL AND TRIM(pan_number) <> '' AND
  aadhar_number IS NOT NULL AND TRIM(aadhar_number) <> '' AND
  city IS NOT NULL AND TRIM(city) <> '' AND
  state IS NOT NULL AND TRIM(state) <> '' AND
  pincode IS NOT NULL AND TRIM(pincode) <> '' AND
  customer_since IS NOT NULL AND TRIM(customer_since) <> '' AND
  kyc_status IS NOT NULL AND TRIM(kyc_status) <> '' AND
  credit_score IS NOT NULL AND TRIM(credit_score) <> '' AND
  risk_category IS NOT NULL AND TRIM(risk_category) <> '' AND
  is_active IS NOT NULL AND TRIM(is_active) <> '';


--Data cleansing, deduplication on transactions.csv file
CREATE OR REFRESH STREAMING TABLE silver.transactions_clean_pl 
(
  CONSTRAINT valid_transaction_id EXPECT (
    transaction_id IS NOT NULL
  ) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT *
FROM stream transactions;

--Data cleansing, deduplication on loans.jsonl file
CREATE OR REFRESH STREAMING TABLE silver.loans_clean_pl  
(
  CONSTRAINT valid_loan_id EXPECT (
    loan_id IS NOT NULL
  ) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT *
FROM stream loans;

--Data cleansing, deduplication on accounts.jsonl file

CREATE OR REFRESH STREAMING TABLE silver.accounts_clean_pl  
(
  CONSTRAINT valid_account_id EXPECT (
    account_id IS NOT NULL
  ) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT *
FROM stream accounts;

--Data cleansing, deduplication on branches.jsonl file

CREATE OR REFRESH STREAMING TABLE silver.branches_clean_pl  
(
  CONSTRAINT valid_branch_code EXPECT (
    branch_code IS NOT NULL
  ) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT *
FROM stream branches;


-- Masking on Customers Email creating new streaming table

CREATE OR REPLACE STREAMING TABLE silver.masked_email_customers 
AS 
SELECT  
  customer_id, 
  first_name, 
  last_name, 
  CONCAT('xxxx', '@', SPLIT(email, '@')[1]) AS email, 
  phone, 
  date_of_birth, 
  gender, 
  annual_income, 
  pan_number, 
  aadhar_number, 
  city, 
  state, 
  pincode, 
  customer_since, 
  kyc_status, 
  credit_score, 
  risk_category, 
  is_active 
FROM STREAM(silver.customers_clean_pl);


--SCD 2 on customers
CREATE OR REFRESH STREAMING TABLE silver.customers_SCD2;
CREATE FLOW customers_flow
AS AUTO CDC INTO
  silver.customers_SCD2
FROM STREAM(silver.customers_clean_pl)
  KEYS (customer_id)
  SEQUENCE BY ingestion_date
  COLUMNS * EXCEPT (_rescued_data, ingestion_date)
  STORED AS SCD TYPE 2;

--SCD2 another code



  

