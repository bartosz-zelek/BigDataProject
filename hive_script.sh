#!/bin/bash

INPUT_PATH_DELAYS="$1"
INPUT_PATH_AIRPORTS="$2"
OUTPUT_PATH="$3"

HIVE_HOST="127.0.0.1"
HIVE_PORT="10000"
HIVE_DB="default"
HIVE_USER="root"
HIVE_PASSWORD=""

# Temporary CSV file to hold query results
TEMP_CSV="/tmp/query_output.csv"

# Run the Beeline query and output as CSV
beeline -u "jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}/${HIVE_DB}" -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" --silent=true --outputformat=csv2 <<EOF >"$TEMP_CSV"

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
location '${INPUT_PATH_DELAYS}';

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
location '${INPUT_PATH_AIRPORTS}'
TBLPROPERTIES ("skip.header.line.count"="1");


DROP VIEW IF EXISTS top_3_states_per_month;
CREATE VIEW top_3_states_per_month AS
SELECT
    year,
    month,
    state,
    AVG(total_delay) AS avg_delay
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
SELECT
    year,
    month,
    state,
    avg_delay
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
ORDER BY
    year,
    month,
    avg_delay DESC;

EOF

# Check if Beeline command was successful
if [ $? -ne 0 ]; then
    echo "Error in Beeline query!"
    exit 1
fi

# Convert CSV output to JSON array format and save in OUTPUT_PATH
awk -F, '
NR > 1 {
    if (NR > 2) printf(",\n")  # Add a comma and newline before each JSON object except the first
    printf("  {\"year\": %s, \"month\": %s, \"state\": \"%s\", \"avg_delay\": %s}", $1, $2, $3, $4)
}
END { print "\n]" }
' "$TEMP_CSV" >"$OUTPUT_PATH"

# Add the opening bracket for the JSON array
sed -i '1i [' "$OUTPUT_PATH"

# Clean up temporary file
rm "$TEMP_CSV"

echo "Success! JSON data saved to ${OUTPUT_PATH}"
