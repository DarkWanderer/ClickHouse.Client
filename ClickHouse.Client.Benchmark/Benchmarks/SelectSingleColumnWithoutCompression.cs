using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class SelectSingleColumnWithoutCompression : IBenchmark
    {
        private readonly string connectionString;

        public virtual string Name => "Select single column without compression";

        public SelectSingleColumnWithoutCompression(string connectionString)
        {
            this.connectionString = connectionString;
        }

        protected virtual bool Compression => false;

        private string GetCustomConnectionString()
        {
            var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = connectionString };
            builder.Compression = Compression;
            return builder.ToString();
        }

        public async Task<BenchmarkResult> Run()
        {
            var stopwatch = new Stopwatch();

            const ulong count = 50000000;
            using var connection = new ClickHouseConnection(GetCustomConnectionString());
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            ulong counter = 0;
            stopwatch.Start();
            while (reader.Read())
                counter++;
            stopwatch.Stop();

            return new BenchmarkResult { RowsCount = counter, DataSize = counter * sizeof(long), Duration = stopwatch.Elapsed };
        }
    }
}
