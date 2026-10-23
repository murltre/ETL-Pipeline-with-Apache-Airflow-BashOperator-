#!/usr/bin/env bash
# Project 1: BashOperator ETL workflow
# Extract username, UID, and home directory from a colon-delimited Linux account file.

set -Eeuo pipefail
IFS=$'\n\t'

# Resolve the project root from this script; no personal machine path is required.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}"
SOURCE_FILE="${SOURCE_FILE:-/etc/passwd}"
INTERMEDIATE_DIR="${PROJECT_ROOT}/data/intermediate"
OUTPUT_DIR="${PROJECT_ROOT}/data/processed"
LOG_DIR="${PROJECT_ROOT}/logs"
EXTRACTED_FILE="${INTERMEDIATE_DIR}/users_extracted.txt"
OUTPUT_FILE="${OUTPUT_DIR}/users.csv"
LOG_FILE="${LOG_DIR}/user_etl.log"
TEMP_OUTPUT=""

# Create output folders and append timestamped run messages to the log.
mkdir -p -- "$INTERMEDIATE_DIR" "$OUTPUT_DIR" "$LOG_DIR"
log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "$LOG_FILE"
}

# Remove an incomplete temporary output and log failures without hiding the exit code.
cleanup() {
    local exit_status=$?
    if [[ -n "$TEMP_OUTPUT" && -f "$TEMP_OUTPUT" ]]; then
        rm -f -- "$TEMP_OUTPUT"
    fi
    if (( exit_status != 0 )); then
        log "ETL failed with exit code ${exit_status}"
    fi
    return "$exit_status"
}
trap cleanup EXIT

# Validate the source before starting extraction.
if [[ ! -r "$SOURCE_FILE" ]]; then
    log "ERROR: source file is missing or unreadable: ${SOURCE_FILE}"
    exit 1
fi

log "ETL job started"

# EXTRACT: select username, numeric user ID, and home-directory fields.
log "Extract phase started"
cut -d ':' -f 1,3,6 "$SOURCE_FILE" > "$EXTRACTED_FILE"
EXTRACTED_ROWS="$(wc -l < "$EXTRACTED_FILE" | tr -d '[:space:]')"
log "Extracted ${EXTRACTED_ROWS} rows"

# TRANSFORM: replace delimiters and add a descriptive CSV header.
log "Transform phase started"
TEMP_OUTPUT="$(mktemp "${OUTPUT_FILE}.tmp.XXXXXX")"
{
    printf 'username,uid,home_directory\n'
    tr ':' ',' < "$EXTRACTED_FILE"
} > "$TEMP_OUTPUT"
log "Transformation completed"

# LOAD: atomically replace the final CSV only after transformation succeeds.
log "Load phase started"
mv -- "$TEMP_OUTPUT" "$OUTPUT_FILE"
TEMP_OUTPUT=""
OUTPUT_ROWS="$(($(wc -l < "$OUTPUT_FILE") - 1))"
log "Loaded ${OUTPUT_ROWS} rows into ${OUTPUT_FILE}"
log "ETL job completed successfully"
