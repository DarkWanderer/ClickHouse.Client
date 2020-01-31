using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class BulkWriteSingleColumnWithoutCompression : AbstractParameterizedBenchmark, IBenchmark
    {
        public BulkWriteSingleColumnWithoutCompression(string connectionString) : base (connectionString)
        {
            Compression = false;
        }

        public override async Task<BenchmarkResult> Run()
        {
            var targetDatabase = "temp";
            var targetTable = $"{targetDatabase}.bulk_insert_test";

            var stopwatch = new Stopwatch();

            // Create database and table for benchmark
            using var targetConnection = GetConnection();
            await targetConnection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS {targetDatabase}");
            await targetConnection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await targetConnection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Memory");

            targetConnection.ChangeDatabase(targetDatabase);

            using var bulkCopyInterface = new ClickHouseBulkCopy(targetConnection)
            {
                DestinationTableName = targetTable,
                BatchSize = 100000,
                MaxDegreeOfParallelism = 4
            };

            var count = Convert.ToInt32(Duration.TotalSeconds * 5000000.0);
            var values = Enumerable.Range(0, count).Select(i => new object[] { (long)i }).ToList();
            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(values);
            stopwatch.Stop();

            // Verify we've written expected number of rows
            //Assert.AreEqual(count, bulkCopyInterface.RowsWritten);
            //Assert.AreEqual(count, Convert.ToInt32(await targetConnection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}")));

            // Clear table after benchmark
            await targetConnection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");

            var rps = (long)count * 1000 / stopwatch.ElapsedMilliseconds;
            return new BenchmarkResult { Duration = stopwatch.Elapsed, RowsCount = Convert.ToUInt64(count), DataSize = Convert.ToUInt64(count) * sizeof(long) };
        }
    }
}
