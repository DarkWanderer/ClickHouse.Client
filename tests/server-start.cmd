@echo off
set TESTS_DIRECTORY=%~dp0

echo -- Stopping and cleaning existing containers
docker stop clickhouse-server
docker rm clickhouse-server --volumes
docker image prune --force

echo -- Pulling latest version
docker pull clickhouse/clickhouse-server

echo -- Starting container with mounts in %TESTS_DIRECTORY%
docker run -d -p 8123:8123 --name clickhouse-server ^
--mount type=bind,source=%TESTS_DIRECTORY%users.d,target=/etc/clickhouse-server/users.d ^
--mount type=bind,source=%TESTS_DIRECTORY%config.d,target=/etc/clickhouse-server/config.d ^
clickhouse/clickhouse-server
