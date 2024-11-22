#!/bin/bash

# Define log files
query_log="output_queries.log"
server_log="output_server.log"

# Loop from query 0 to 42
for i in {0..42}
do
    # Kill any existing processes listening on port 50051
    lsof -ti:50051 | xargs -r kill -9
    # Start the server in background and capture its PID
    RUST_LOG=cache_server=info \
    cargo run --profile release-nonlto --bin cache_server \
    -- --path benchmarks/data/hits.parquet >> "$server_log" 2>&1 &
    server_pid=$!
    
    # Give the server a moment to start up
    sleep 1
    
    # Run the query
    echo "Running query $i..." | tee -a $query_log
    cargo run --profile release-nonlto --bin dfbench -- clickbench \
    --queries-path benchmarks/queries/clickbench/queries.sql \
    --iterations 4 --path benchmarks/data/hits.parquet \
    --query $i --pushdown-filters \
    --flight-cache http://localhost:50051 &>> $query_log
    
    query_status=$?
    
    # Kill the server
    kill $server_pid
    wait $server_pid 2>/dev/null
    
    # Check query status
    if [ $query_status -eq 0 ]; then
        echo "Query $i completed successfully." | tee -a $query_log
    else
        echo "Query $i failed. Check $query_log for details." | tee -a $query_log
    fi
    
    # Clean up cache
    rm -rf target/arrow-cache
done
