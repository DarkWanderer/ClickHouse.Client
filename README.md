# ClickHouse.Client

Unofficial ADO.NET client for [ClickHouse](https://github.com/ClickHouse/ClickHouse)®, ultra-fast 'big data' relational database

Not affiliated with or endorsed by ClickHouse Inc.

[![Latest version](https://img.shields.io/nuget/v/ClickHouse.Client)](https://www.nuget.org/packages/ClickHouse.Client/)
[![Downloads](https://img.shields.io/nuget/dt/ClickHouse.Client)](https://www.nuget.org/packages/ClickHouse.Client/)
[![codecov](https://codecov.io/gh/DarkWanderer/ClickHouse.Client/branch/main/graph/badge.svg?token=MRkagqhnRh)](https://codecov.io/gh/DarkWanderer/ClickHouse.Client)
[![License](https://img.shields.io/github/license/DarkWanderer/ClickHouse.Client)](https://github.com/DarkWanderer/ClickHouse.Client/blob/main/LICENSE)
[![Tests](https://github.com/DarkWanderer/ClickHouse.Client/actions/workflows/tests.yml/badge.svg)](https://github.com/DarkWanderer/ClickHouse.Client/actions/workflows/tests.yml)

## Key features

* High-throughput
* Fully supports ClickHouse-specific types:
  * Composite types: `Array`, `Tuple`, `Nullable`, `Nested`, `Map`, including combinations
  * Specialized types: `IPv4`, `IPv6`, `UUID`, `DateTime64`, `LowCardinality`, `Enum` etc.
  * Large arithmetic types: `(U)Int128`, `(U)Int256`, `Decimal128`, `Decimal256`
* Correctly handles `DateTime`, including time zones
* Supports [bulk insertion](https://github.com/DarkWanderer/ClickHouse.Client/wiki/Bulk-insertion)
* Uses compressed binary protocol over HTTP(S)
* Available for .NET Core/Framework/Standard

## Advantages

Compared to other existing .NET clients, `ClickHouse.Client` has following advantages 
* Does not have to buffer response, reducing memory usage
* Offers wider support for ClickHouse-specific types
* Is more compliant to ADO.NET standards (e.g. does not require calling 'NextResult' on `SELECT` queries)
* Works with ORM like Dapper, Linq2DB, Entity Framework Core etc.

## Documentation

Documentation for the library is available in [repository Wiki](https://github.com/DarkWanderer/ClickHouse.Client/wiki)

## Acknowledgements

* Originally created by [Oleg V. Kozlyuk](https://github.com/DarkWanderer)
* ClickHouse is a registered trademark of ClickHouse, Inc. https://clickhouse.com
