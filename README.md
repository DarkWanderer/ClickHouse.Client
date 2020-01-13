# ClickHouse.Client

.NET ADO.NET driver for ClickHouse

[![License](https://img.shields.io/github/license/DarkWanderer/ClickHouse.Client?style=plastic)](https://github.com/DarkWanderer/ClickHouse.Client/blob/master/LICENSE)
[![Build status](https://img.shields.io/appveyor/ci/DarkWanderer/clickhouse-client/master?style=plastic)](https://ci.appveyor.com/project/DarkWanderer/clickhouse-client/branch/master)
[![NuGet status](https://img.shields.io/nuget/dt/ClickHouse.Client?style=plastic)](https://www.nuget.org/packages/ClickHouse.Client/)

## Key features

* Uses HTTP, so is compatible with any server version
* Supports Binary, JSON, TSV protocols (selectable via Driver connection string parameter)
* Supports all recursive types (`Array(Nullable(Int32))` etc.)
* High-throughput
