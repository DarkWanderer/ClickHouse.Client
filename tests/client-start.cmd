docker run -it --rm --link clickhouse-server:clickhouse-server --entrypoint clickhouse-client clickhouse/clickhouse-server --host clickhouse-server
