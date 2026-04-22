-- Keep all project work under a dedicated database so cleanup stays simple.
CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

-- The source file is comma separated and already carries a header row.
DROP TABLE IF EXISTS client_info;

CREATE EXTERNAL TABLE client_info (
    age INT,
    job STRING,
    marital STRING,
    education STRING,
    `default` STRING,
    balance INT,
    housing STRING,
    loan STRING,
    contact STRING,
    day INT,
    month STRING,
    duration INT,
    campaign INT,
    pdays INT,
    previous INT,
    poutcome STRING,
    y STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/project/banking/raw'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Basic data exploration
SELECT COUNT(*) AS total_clients
FROM client_info;

SELECT *
FROM client_info
LIMIT 10;

-- Data filtering and sorting
SELECT *
FROM client_info
WHERE marital = 'married' AND loan = 'yes';

SELECT job, marital, balance
FROM client_info
ORDER BY balance DESC
LIMIT 10;

-- Data aggregation and grouping
SELECT job, ROUND(AVG(age), 2) AS average_age
FROM client_info
GROUP BY job
ORDER BY average_age DESC;

SELECT education, COUNT(*) AS defaulted_clients
FROM client_info
WHERE `default` = 'yes'
GROUP BY education
ORDER BY defaulted_clients DESC;

-- Complex queries for insights
WITH top_jobs AS (
    SELECT job, AVG(balance) AS avg_balance
    FROM client_info
    GROUP BY job
    ORDER BY avg_balance DESC
    LIMIT 5
)
SELECT
    c.job,
    ROUND(AVG(c.balance), 2) AS average_balance,
    ROUND(100 * AVG(CASE WHEN c.y = 'yes' THEN 1 ELSE 0 END), 2) AS subscription_rate
FROM client_info c
JOIN top_jobs t
    ON c.job = t.job
GROUP BY c.job
ORDER BY average_balance DESC;

WITH month_counts AS (
    SELECT
        month,
        COUNT(*) AS contact_count,
        ROUND(100 * AVG(CASE WHEN y = 'yes' THEN 1 ELSE 0 END), 2) AS success_rate
    FROM client_info
    GROUP BY month
)
SELECT *
FROM month_counts
ORDER BY contact_count DESC
LIMIT 1;

-- Correlation analysis
SELECT CORR(CAST(age AS DOUBLE), CAST(balance AS DOUBLE)) AS age_balance_correlation
FROM client_info;

-- The dataset does not include a year field, so month-wise trend is used as the closest time-based view.
SELECT
    month,
    COUNT(*) AS clients_contacted
FROM client_info
GROUP BY month
ORDER BY
    CASE month
        WHEN 'jan' THEN 1
        WHEN 'feb' THEN 2
        WHEN 'mar' THEN 3
        WHEN 'apr' THEN 4
        WHEN 'may' THEN 5
        WHEN 'jun' THEN 6
        WHEN 'jul' THEN 7
        WHEN 'aug' THEN 8
        WHEN 'sep' THEN 9
        WHEN 'oct' THEN 10
        WHEN 'nov' THEN 11
        WHEN 'dec' THEN 12
        ELSE 13
    END;

-- The source file also lacks a year column, so monthly balance spikes are used to flag unusual patterns.
WITH monthly_balance AS (
    SELECT
        education,
        month,
        AVG(balance) AS monthly_avg_balance
    FROM client_info
    GROUP BY education, month
),
education_stats AS (
    SELECT
        education,
        AVG(monthly_avg_balance) AS mean_balance,
        STDDEV_POP(monthly_avg_balance) AS std_balance
    FROM monthly_balance
    GROUP BY education
),
education_anomalies AS (
    SELECT
        m.education,
        m.month,
        m.monthly_avg_balance,
        (m.monthly_avg_balance - s.mean_balance) / CASE
            WHEN s.std_balance = 0 OR s.std_balance IS NULL THEN NULL
            ELSE s.std_balance
        END AS z_score
    FROM monthly_balance m
    JOIN education_stats s
        ON m.education = s.education
)
SELECT
    education,
    month,
    ROUND(monthly_avg_balance, 2) AS avg_balance,
    ROUND(z_score, 2) AS z_score
FROM education_anomalies
WHERE z_score IS NOT NULL
  AND ABS(z_score) >= 1.5
ORDER BY ABS(z_score) DESC;

-- Advanced analysis
SELECT
    poutcome,
    ROUND(100 * AVG(CASE WHEN y = 'yes' THEN 1 ELSE 0 END), 2) AS subscription_rate
FROM client_info
GROUP BY poutcome
ORDER BY subscription_rate DESC;

SELECT
    y,
    ROUND(AVG(duration), 2) AS average_duration
FROM client_info
GROUP BY y
ORDER BY y;
