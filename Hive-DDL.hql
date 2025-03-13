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
