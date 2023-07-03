using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks;

public class SelectColumnBenchmarks
{
    private readonly ClickHouseConnection connection;

    [Params(100000)]
    public int Count { get; set; }

    public SelectColumnBenchmarks()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);
    }

    [Benchmark(Baseline = true)]
    public async Task SelectInt32()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectUInt32()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toUInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDecimal64()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDecimal64(number,5) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDecimal128()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDecimal128(number,5) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDecimal256()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDecimal256(number,5) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDate()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDate(18942+number) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDate32()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDate32(18942+number) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectDateTime()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectString()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectArray()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT array(1, number, 3) FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }
}
