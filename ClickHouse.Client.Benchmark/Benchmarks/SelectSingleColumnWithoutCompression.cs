using System;
using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class SelectSingleColumnWithoutCompression : AbstractParameterizedBenchmark, IBenchmark
    {
        public SelectSingleColumnWithoutCompression(string connectionString) : base(connectionString)
        {
            Compression = false;
        }

        public override async Task<BenchmarkResult> Run()
        {
            var stopwatch = new Stopwatch();
            ulong maxCount = int.MaxValue / 32;
            ulong counter = 0;

            using var connection = GetConnection();
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers");

            var totalMilliseconds = Convert.ToInt64(Duration.TotalMilliseconds);
            stopwatch.Start();
            while (reader.Read() && counter < maxCount)
                counter++;
            stopwatch.Stop();

            return new BenchmarkResult { RowsCount = counter, DataSize = counter * sizeof(long), Duration = stopwatch.Elapsed };
        }
    }
}
