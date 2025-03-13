#!/bin/bash

# Check if a date argument is provided
if [ $# -ne 1 ]; then
  echo "Usage: $0 <date>"
  echo "Example: $0 2023-09-01"
  exit 1
fi

DATE=$1
YEAR=$(echo $DATE | cut -d '-' -f 1)
MONTH=$(echo $DATE | cut -d '-' -f 2)
DAY=$(echo $DATE | cut -d '-' -f 3)

# Define HDFS paths
LOGS_HDFS_PATH="/raw/logs/$YEAR/$MONTH/$DAY"
METADATA_HDFS_PATH="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create HDFS directories (if they don't exist)
hdfs dfs -mkdir -p "$LOGS_HDFS_PATH"
hdfs dfs -mkdir -p "$METADATA_HDFS_PATH"

# Copy CSV files to HDFS
hdfs dfs -put /tmp/streaming_user_activity_logs.csv "$LOGS_HDFS_PATH/streaming_user_activity_logs.csv"
hdfs dfs -put /tmp/streaming_content_metadata.csv "$METADATA_HDFS_PATH/streaming_content_metadata.csv"

# Hive CLI command to create tables (if they don't exist) and load data from HDFS
hive -e "
CREATE TABLE IF NOT EXISTS streaming_content_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '$METADATA_HDFS_PATH';

CREATE TABLE IF NOT EXISTS streaming_user_activity_logs (
    user_id INT,
    content_id INT,
    action STRING,
    timestamp STRING,
    device STRING,
    region STRING,
    session_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '$LOGS_HDFS_PATH';

-- Example query to verify data and filter by date (if applicable)
SELECT * FROM streaming_user_activity_logs WHERE timestamp LIKE '$DATE%';
"

echo "Data ingested for $DATE into HDFS paths:"
echo "$LOGS_HDFS_PATH"
echo "$METADATA_HDFS_PATH"