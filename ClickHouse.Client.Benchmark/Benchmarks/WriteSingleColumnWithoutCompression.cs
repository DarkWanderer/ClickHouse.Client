using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class BulkWriteSingleColumnWithCompression : AbstractParameterizedBenchmark, IBenchmark
    {
        public BulkWriteSingleColumnWithCompression(string connectionString) : base(connectionString)
        {
            Compression = true;
        }

        public override async Task<BenchmarkResult> Run()
        {
            var count = int.MaxValue / 32;
            Console.WriteLine("Preparing data");
            var values = Enumerable.Range(0, count).Select(i => new object[] { (long)i }).ToList();
            Console.WriteLine("Running benchmark");

            var targetDatabase = "benchmark";
            var targetTable = $"{targetDatabase}.bulk_insert_test";

            var stopwatch = new Stopwatch();

            // Create database and table for benchmark
            using var targetConnection = GetConnection();
            await targetConnection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS {targetDatabase}");
            await targetConnection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Null");

            targetConnection.ChangeDatabase(targetDatabase);

            using var bulkCopyInterface = new ClickHouseBulkCopy(targetConnection)
            {
                DestinationTableName = targetTable,
                BatchSize = 100000,
                MaxDegreeOfParallelism = 1
            };

            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(values);
            stopwatch.Stop();

            // Verify we've written expected number of rows
            //Assert.AreEqual(count, bulkCopyInterface.RowsWritten);
            //Assert.AreEqual(count, Convert.ToInt32(await targetConnection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}")));

            // Clear table after benchmark

            var rps = (long)count * 1000 / stopwatch.ElapsedMilliseconds;
            return new BenchmarkResult { Duration = stopwatch.Elapsed, RowsCount = Convert.ToUInt64(count), DataSize = Convert.ToUInt64(count) * sizeof(long) };
        }
    }
}
