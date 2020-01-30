using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Benchmark.Benchmarks;

namespace ClickHouse.Client.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");

            var benchmarks = new List<IBenchmark>()
            {
                //new SelectSingleColumnWithoutCompression(connectionString),
                //new SelectSingleColumnWithCompression(connectionString),
                new BulkWriteSingleColumnWithoutCompression(connectionString)
            };
            foreach (var benchmark in benchmarks)
            {
                Console.WriteLine($"Running benchmark '{benchmark.Name}'");
                var result = await benchmark.Run();
                Console.WriteLine($"{result.DataThroughput:#,##} bytes/s");
                Console.WriteLine($"{result.RowsThroughput:#,##} rows/s");
            }
        }
    }
}
