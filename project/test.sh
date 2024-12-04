#!/bin/bash

# Setting up variables
OUTPUT_DB="../data/sp500_data.db"
TABLE1="sp500_companies"
TABLE2="sp500_stocksprice_and_volume"
LOG_FILE="ETLPipeline.log"


# Function to check Kaggle API
check_kaggle_api() {
    if pip3 show kaggle >/dev/null 2>&1; then
        echo "Kaggle API is installed."
    else
        echo "Kaggle API is not installed. Install it using 'pip install kaggle'."
        exit 1
    fi
}

# Function to clean up previous artifacts
cleanup() {
    echo "Cleaning up previous artifacts..."
    [ -f "$OUTPUT_DB" ] && rm "$OUTPUT_DB" && echo "Removed existing database: $OUTPUT_DB"
    [ -f "$LOG_FILE" ] && rm "$LOG_FILE" && echo "Removed existing log file: $LOG_FILE"
    echo "Cleanup completed."
}

# Function to run the ETL pipeline
run_ETLPipeline() {
    echo "Executing the ETL pipeline..."
    python3 ETLPipeline.py || error_exit "ETL pipeline execution failed."
}

# Function to validate the output database and tables
validate_output() {
    echo "Validating output database and tables..."
    if [ -f "$OUTPUT_DB" ]; then
        echo "PASS: Output database exists: $OUTPUT_DB"
        # Check for the presence of tables
        TABLE1_EXISTS=$(sqlite3 "$OUTPUT_DB" "SELECT name FROM sqlite_master WHERE type='table' AND name='$TABLE1';")
        TABLE2_EXISTS=$(sqlite3 "$OUTPUT_DB" "SELECT name FROM sqlite_master WHERE type='table' AND name='$TABLE2';")
        if [ "$TABLE1_EXISTS" == "$TABLE1" ]; then
            echo "PASS: Table '$TABLE1' exists."
        else
            error_exit "FAIL: Table '$TABLE1' does not exist."
        fi

        if [ "$TABLE2_EXISTS" == "$TABLE2" ]; then
            echo "PASS: Table '$TABLE2' exists."
        else
            error_exit "FAIL: Table '$TABLE2' does not exist."
        fi
    else
        error_exit "FAIL: Output database not found: $OUTPUT_DB"
    fi
}

# Function to display logs if they exist
display_logs() {
    if [ -f "$LOG_FILE" ]; then
        echo "Displaying logs:"
        cat "$LOG_FILE"
    else
        echo "No log file found."
    fi
}

# Main script execution
main() {
  
    check_kaggle_api
    cleanup
    run_ETLPipeline
    validate_output
    display_logs
    echo "All tests passed successfully."
}

# Execute the main function
main