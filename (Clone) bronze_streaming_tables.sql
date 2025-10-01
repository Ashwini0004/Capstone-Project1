-- create streaming table 
--Customers.csv
create streaming table customers as
select *, current_date() as ingestion_date from stream read_files("/Volumes/capstone_project/bronze/raw/customers/", format => "csv");


--transactions.csv
create or refresh streaming table transactions as
select * from stream read_files("/Volumes/capstone_project/bronze/raw/transactions/", format => "csv");


--branches.json
CREATE STREAMING TABLE branches AS 
SELECT * 
FROM STREAM read_files(
  "/Volumes/capstone_project/bronze/raw/branches/",
  format => 'json',
  multiLine => 'true'
);


--loans.json
CREATE STREAMING TABLE loans AS 
SELECT * 
FROM STREAM read_files(
  "/Volumes/capstone_project/bronze/raw/loans/",
  format => 'json'
);


--accounts.json
CREATE STREAMING TABLE accounts AS
SELECT
  -- Select all columns except the three to be casted
  * EXCEPT (created_timestamp, opening_date, last_transaction_date),
  -- Add the casted columns
  CAST(created_timestamp AS DATE) AS created_timestamp,
  CAST(opening_date AS DATE) AS opening_date,
  CAST(last_transaction_date AS DATE) AS last_transaction_date
FROM STREAM read_files(
  "/Volumes/capstone_project/bronze/raw/accounts/",
  format => 'json',
  multiLine => 'true'
);

