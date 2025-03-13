---RAW DDL

CREATE TABLE streaming_user_activity_logs (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp STRING, -- Renamed column
    device STRING,
    region STRING,
    session_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '$LOGS_HDFS_PATH';

CREATE EXTERNAL TABLE external_streaming_content_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata';

-- Create the User Dimension Table
CREATE TABLE dim_users (
    user_id INT,
    user_name STRING,
    subscription_type STRING,
    region STRING
)
STORED AS PARQUET;

-- Create the Content Dimension Table
CREATE TABLE dim_content (
    content_id INT,
    title STRING,
    genre STRING,
    release_year INT
)
STORED AS PARQUET;

-- Create the Time Dimension Table
CREATE TABLE dim_time (
    date_key STRING,
    year INT,
    month INT,
    day INT,
    weekday STRING
)
STORED AS PARQUET;

-- Create the Fact Table (Streaming Activity)
CREATE TABLE fact_streaming_activity (
    activity_id BIGINT,
    user_id INT,
    content_id INT,
    date_key STRING,  -- Foreign Key to dim_time
    watch_duration INT,  -- Watch time in minutes
    device STRING
)
STORED AS PARQUET;

CREATE TABLE fact_user_actions (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp TIMESTAMP,
    device STRING,
    region STRING,
    session_id STRING,
    year INT,
    month INT,
    day INT
)
PARTITIONED BY (year, month, day)
STORED AS PARQUET;

---DATA TRANSFORMATION

CREATE EXTERNAL TABLE external_streaming_user_activity_logs (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp STRING,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs';

CREATE EXTERNAL TABLE external_streaming_content_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata';

INSERT OVERWRITE TABLE dim_content
SELECT
    content_id,
    title,
    category,
    length,
    artist
FROM
    external_streaming_content_metadata;

INSERT OVERWRITE TABLE dim_users
SELECT
    user_id,
    user_name,
    subscription_type,
    region
FROM
    external_user_data;

INSERT OVERWRITE TABLE fact_user_actions
PARTITION (year, month, day)
SELECT
    user_id,
    content_id,
    action,
    CAST(event_timestamp AS TIMESTAMP), -- Convert to timestamp
    device,
    region,
    session_id,
    YEAR(CAST(event_timestamp AS TIMESTAMP)),
    MONTH(CAST(event_timestamp AS TIMESTAMP)),
    DAY(CAST(event_timestamp AS TIMESTAMP))
FROM
    external_streaming_user_activity_logs;

---ANALYTICAL QUERIES

---Active Users by region in 2023
SELECT
    year, month, region, COUNT(DISTINCT user_id) AS monthly_active_users
FROM
    fact_user_actions
WHERE
    year = 2023
GROUP BY
    year, month, region
ORDER BY
    year, month, monthly_active_users DESC;

---Top 10 categories by play count
SELECT
    dc.category,
    COUNT(*) AS play_count
FROM
    fact_user_actions fua
JOIN
    dim_content dc ON fua.content_id = dc.content_id
WHERE
    fua.action = 'play'
    AND fua.year = 2023
    AND fua.month = 9 --replace with desired month
GROUP BY
    dc.category
ORDER BY
    play_count DESC
LIMIT 10;

---Most popular content on weekends
SELECT 
    u.user_name, 
    COUNT(f.activity_id) AS activities
FROM fact_streaming_activity f
JOIN dim_users u ON f.user_id = u.user_id
JOIN dim_time t ON f.date_key = t.date_key
WHERE t.weekday IN ('Saturday', 'Sunday')
GROUP BY u.user_name
ORDER BY activities DESC
LIMIT 10;



