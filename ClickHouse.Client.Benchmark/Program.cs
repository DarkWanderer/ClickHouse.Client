using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Benchmark.Benchmarks;

namespace ClickHouse.Client.Benchmark
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");

            var benchmarks = new List<IBenchmark>()
            {
                new SelectSingleColumnWithoutCompression(connectionString),
                new SelectSingleColumnWithCompression(connectionString),
                new BulkWriteSingleColumnWithoutCompression(connectionString),
                new BulkWriteSingleColumnWithCompression(connectionString),
            };
            foreach (var benchmark in benchmarks)
            {
                Console.WriteLine($"Running benchmark '{benchmark.GetType().Name}'");
                var result = await benchmark.Run();
                Print(result);
            }
        }

        private static void Print(BenchmarkResult result)
        {
            Console.WriteLine($"{result.Duration.TotalSeconds:#,#} seconds");
            Console.WriteLine($"{result.DataThroughput:#,#} bytes/s");
            Console.WriteLine($"{result.RowsThroughput:#,#} rows/s");

        }
    }
}
