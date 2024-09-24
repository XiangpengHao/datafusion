#!/bin/bash

# Define the log file
log_file="output_queries.log"

# Loop from query 0 to 42
for i in {0..42}
do
    # Define the command
    command="cargo run --profile release-nonlto --bin dfbench -- clickbench --queries-path benchmarks/queries/clickbench/queries.sql --iterations 4 --path benchmarks/data/hits.parquet --query $i"
    
    # Run the command and capture the output to the log file
    echo "Running query $i..." | tee -a $log_file
    $command &>> $log_file
    
    # Check if the command succeeded or failed
    if [ $? -eq 0 ]; then
        echo "Query $i completed successfully." | tee -a $log_file
    else
        echo "Query $i failed. Check $log_file for details." | tee -a $log_file
    fi
done
