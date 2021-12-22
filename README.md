# ClickHouse.Client

ADO.NET client for [ClickHouse](https://github.com/ClickHouse/ClickHouse), ultra-fast 'big data' relational database

[![Latest version](https://img.shields.io/nuget/v/ClickHouse.Client)](https://www.nuget.org/packages/ClickHouse.Client/)
[![Downloads](https://img.shields.io/nuget/dt/ClickHouse.Client)](https://www.nuget.org/packages/ClickHouse.Client/)
[![License](https://img.shields.io/github/license/DarkWanderer/ClickHouse.Client)](https://github.com/DarkWanderer/ClickHouse.Client/blob/master/LICENSE)
![Regression](https://github.com/DarkWanderer/ClickHouse.Client/workflows/Regression/badge.svg)

## Key features

* High-throughput
* Fully supports ClickHouse-specific types:
  * Composite types: `Array`, `Tuple`, `Nullable`, `Nested`, `Map`, including combinations
  * Specialized types: `IPv4`, `IPv6`, `UUID`, `DateTime64`, `LowCardinality`, `Enum` etc.
* Correctly handles `DateTime`, including time zones
* Uses compressed binary protocol over HTTP(S)
* Supports [bulk insertion](https://github.com/DarkWanderer/ClickHouse.Client/wiki/Bulk-insertion)
* Uses HTTP(S), so is compatible with any server version
* Available for .NET Core/Framework/Standard

## Why another client?

Compared to other existing .NET clients, `ClickHouse.Client` has following advantages 
* Does not have to buffer response, reducing memory usage
* Is version-agnostic
* Offers wider support for ClickHouse-specific types
* Is more compliant to ADO.NET standards (e.g. does not require calling 'NextResult' on `SELECT` queries)

## Documentation

Documentation for the library is available in [repository Wiki](https://github.com/DarkWanderer/ClickHouse.Client/wiki)
