using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks;

public class BulkInsertColumnBenchmark
{
    private readonly ClickHouseConnection connection;
    private readonly List<object[]> data = new();
    private readonly ClickHouseBulkCopy bulkCopy;
    const int count = 1000000;

    public BulkInsertColumnBenchmark()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);


        var random = new Random(42);
        data.EnsureCapacity(count);
        for (int i = 0; i < data.Capacity; i++)
        {
            data.Add(new object[] { random.Next() });
        }
        
        var targetTable = $"test.bulk_insert_benchmark";

        // Create database and table for benchmark
        connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}").Wait();
        connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Null").Wait();

        bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            BatchSize = 10000,
            MaxDegreeOfParallelism = 1
        };
    }

    [Benchmark]
    public async Task BulkInsertInt32() => await bulkCopy.WriteToServerAsync(data);
}
