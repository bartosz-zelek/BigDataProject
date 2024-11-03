-- Define parameters for input paths and output path
SET hivevar:input_path_delays=${hivevar:input_path_delays};
SET hivevar:input_path_airports=${hivevar:input_path_airports};
SET hivevar:output_path=${hivevar:output_path};

-- Create external table for landings delays data
CREATE EXTERNAL TABLE IF NOT EXISTS external_airports_landings_delays (
    AIRPORT STRING,
    YEAR STRING,
    MONTH STRING,
    LANDINGS_COUNT INT,
    TOTAL_DELAY INT
)
COMMENT "mapreduce output"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "([^,]+),([^,]+),([^\\t]+)\\t([^,]+),([^\\s]+)"
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_path_delays}';

-- Create external table for airports data
CREATE EXTERNAL TABLE IF NOT EXISTS external_airports (
    IATA STRING,
    AIRPORT STRING,
    CITY STRING,
    STATE STRING,
    COUNTRY STRING,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:input_path_airports}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Drop view if exists
DROP VIEW IF EXISTS top_3_states_per_month;

-- Create view for top 3 states by average delay per month
CREATE VIEW top_3_states_per_month AS
SELECT
    year,
    month,
    state,
    SUM(total_delay) / SUM(ald.landings_count) AS avg_delay
FROM
    external_airports_landings_delays ald
JOIN
    external_airports a
ON
    ald.airport = a.iata
GROUP BY
    year,
    month,
    state
DISTRIBUTE BY
    year, month
SORT BY
    year, month, avg_delay DESC;

-- Query to select top 3 states by average delay per month
INSERT OVERWRITE DIRECTORY '${hivevar:output_path}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT 
    CONCAT(
        '{',
        '"year": "', year, '", ',
        '"month": "', month, '", ',
        '"state": "', state, '", ',
        '"avg_delay": ', CAST(avg_delay AS STRING),
        '}'
    ) AS json_output 
FROM
    (
        SELECT
            year,
            month,
            state,
            avg_delay,
            ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY avg_delay DESC) AS rank
        FROM
            top_3_states_per_month
    ) ranked_states
WHERE
    rank <= 3

