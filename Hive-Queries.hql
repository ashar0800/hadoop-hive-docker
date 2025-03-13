---RAW DDL

CREATE TABLE IF NOT EXISTS streaming_user_activity_logs (
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

