# ClickHouse.Client

.NET ADO.NET driver for ClickHouse

[![License](https://img.shields.io/github/license/DarkWanderer/ClickHouse.Client?style=plastic)](https://github.com/DarkWanderer/ClickHouse.Client/blob/master/LICENSE)
[![NuGet status](https://img.shields.io/nuget/dt/ClickHouse.Client?style=plastic)](https://www.nuget.org/packages/ClickHouse.Client/)
[![Build status](https://ci.appveyor.com/api/projects/status/2tex8lslgd93ha9l/branch/master?svg=true)](https://ci.appveyor.com/project/DarkWanderer/clickhouse-client/branch/master)

## What is it?

ADO.NET client for [ClickHouse](https://github.com/ClickHouse/ClickHouse), ultra-fast 'big data' relational database

## Why another client?

Other existing clients for .NET utilize ClickHouse 'native' protocol. It has following disadvantages:
* Has to buffer response (due to native format being columnar)
* May break with ClickHouse version upgrade
* May return multiple table results for one SELECT statement

Row-based binary format used in this client does not have these problems. Also, this client is ADO-compliant and does not require users to call `NextResult`

## Key features

* Uses HTTP, so is compatible with any server version
* Uses binary protocol for communication. Optionaly supports JSON/TSV protocols see [connection string](https://github.com/DarkWanderer/ClickHouse.Client/wiki/Connection-string) 
* Supports parameterized types, including recursive packing (`Array(Nullable(Int32))` etc.)
* High-throughput
* Available for .NET Core/Framework/Standard
* [Bulk insert](https://github.com/DarkWanderer/ClickHouse.Client/wiki/Bulk-insertion) support
