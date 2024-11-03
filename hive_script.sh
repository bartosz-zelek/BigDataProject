#!/bin/bash

INPUT_PATH_DELAYS="$1"
INPUT_PATH_AIRPORTS="$2"
OUTPUT_PATH="$3"

HIVE_HOST="127.0.0.1"
HIVE_PORT="10000"
HIVE_DB="default"
HIVE_USER="root"
HIVE_PASSWORD=""

beeline -u "jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}/${HIVE_DB}" -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" --hivevar input_path_delays="$INPUT_PATH_DELAYS" --hivevar input_path_airports="$INPUT_PATH_AIRPORTS" --hivevar output_path="$OUTPUT_PATH" -f hive_script.hql
