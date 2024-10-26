CREATE EXTERNAL TABLE AIRPORTS_LANDINGS_DELAYS (
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
location 'hdfs://namenode:9000/user/root/input/mr';

-- hadoop fs -ls hdfs://namenode:9000/ - check files in hdfs

CREATE EXTERNAL TABLE AIRPORTS (
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
location 'hdfs://namenode:9000/user/root/input/datasource4'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Działając na wyniku zadania MapReduce (3) oraz zbiorze danych datasource4 (4) należy dla każdego miesiąca wyznaczyć trzy stany o największym średnim opóźnieniu przylotów.

-- Ostateczny wynik (6) powinien zawierać następujące atrybuty:

-- year - rok
-- month – miesiąc
-- state – nazwa stanu
-- avg_delay – średnie opóźnienie

-- WITH RankedDelays AS (
--     SELECT
--         YEAR,
--         MONTH,
--         STATE,
--         AVG(TOTAL_DELAY) AS AVG_DELAY,
--         ROW_NUMBER() OVER (PARTITION BY YEAR, MONTH ORDER BY AVG(TOTAL_DELAY) DESC) AS RANK
--     FROM
--         AIRPORTS_LANDINGS_DELAYS
--     JOIN
--         AIRPORTS
--     ON
--         AIRPORTS_LANDINGS_DELAYS.AIRPORT = AIRPORTS.IATA
--     GROUP BY
--         YEAR,
--         MONTH,
--         STATE
-- )
-- SELECT
--     YEAR,
--     MONTH,
--     STATE,
--     AVG_DELAY
-- FROM
--     RankedDelays
-- WHERE
--     RANK <= 3
-- ORDER BY
--     YEAR,
--     MONTH,
--     AVG_DELAY DESC;

CREATE OR REPLACE VIEW TOP_3_STATES_PER_MONTH as
SELECT
    YEAR,
    MONTH,
    STATE,
    AVG(TOTAL_DELAY) AS AVG_DELAY
FROM
    AIRPORTS_LANDINGS_DELAYS
JOIN
    AIRPORTS
ON
    AIRPORTS_LANDINGS_DELAYS.AIRPORT = AIRPORTS.IATA
GROUP BY
    YEAR,
    MONTH,
    STATE
DISTRIBUTE BY -- DISTRIBUTE BY - rozdystrybuuje dane do różnych reducerów w zależności od wartości kolumn YEAR, MONTH
    YEAR, MONTH
SORT BY
    YEAR, MONTH, AVG_DELAY DESC
;

SELECT
    YEAR,
    MONTH,
    STATE,
    AVG_DELAY
FROM
    (
        SELECT
            YEAR,
            MONTH,
            STATE,
            AVG_DELAY,
            ROW_NUMBER() OVER (PARTITION BY YEAR, MONTH ORDER BY AVG_DELAY DESC) AS RANK
        FROM
            TOP_3_STATES_PER_MONTH
    ) temp
WHERE
    RANK <= 3
ORDER BY
    YEAR,
    MONTH,
    AVG_DELAY DESC;
