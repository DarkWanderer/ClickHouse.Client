# ClickHouse.Client

.NET ADO.NET driver for ClickHouse

Key features:

* Uses HTTP protocol internally, so is version-agnostic with respect to server
* Supports Binary, JSON, TSV protocols (selectable via Driver connection string parameter)
* Has a 'bulk copy' interface with up to 1.5 million values per second performance

# Build status (master)
[![Build status](https://ci.appveyor.com/api/projects/status/2tex8lslgd93ha9l/branch/master?svg=true)](https://ci.appveyor.com/project/DarkWanderer/clickhouse-client/branch/master)
