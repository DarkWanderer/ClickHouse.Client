using System;
using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks;

internal class SelectSingleColumnWithoutCompression : AbstractParameterizedBenchmark, IBenchmark
{
    public SelectSingleColumnWithoutCompression(string connectionString) : base(connectionString)
    {
        Compression = false;
    }

    public override async Task<BenchmarkResult> Run()
    {
        var stopwatch = new Stopwatch();

        using var connection = GetConnection();
        using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers");

        var totalMilliseconds = Convert.ToInt64(Duration.TotalMilliseconds);
        ulong counter = 0;
        stopwatch.Start();
        while (reader.Read() && stopwatch.ElapsedMilliseconds < totalMilliseconds)
            counter++;
        stopwatch.Stop();

        return new BenchmarkResult { RowsCount = counter, DataSize = counter * sizeof(long), Duration = stopwatch.Elapsed };
    }
}
