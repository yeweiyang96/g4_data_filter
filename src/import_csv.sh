# parallelism 6 (-P6)

find . -type f -name '*.csv' | xargs -P 6 -n 1 ./clickhouse --input_format_parallel_parsing=0 --receive_timeout=30000 --input_format_allow_errors_num=999999999 --host "127.0.0.1" --database "default" --port 9000 --user "default" --query "INSERT INTO ... FORMAT CSV"