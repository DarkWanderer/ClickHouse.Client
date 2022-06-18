docker run -d -p 8123:8123 --name clickhouse-server ^
--mount type=bind,source=C:\Users\Oleg\Projects\ClickHouse.Client\tests\users.d,target=/etc/clickhouse-server/users.d ^
--mount type=bind,source=C:\Users\Oleg\Projects\ClickHouse.Client\tests\config.d,target=/etc/clickhouse-server/config.d ^
clickhouse/clickhouse-server
