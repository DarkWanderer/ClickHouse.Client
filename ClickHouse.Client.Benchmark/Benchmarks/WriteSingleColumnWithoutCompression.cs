using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class BulkWriteSingleColumnWithoutCompression : IBenchmark
    {
        private readonly string connectionString;

        public BulkWriteSingleColumnWithoutCompression(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public string Name => "Write single column without compression";

        public async Task<BenchmarkResult> Run()
        {
            const ulong count = 50 * 1000 * 1000;
            var targetDatabase = "temp";
            var targetTable = $"{targetDatabase}.bulk_insert_test";

            var stopwatch = new Stopwatch();

            // Create database and table for benchmark
            using var targetConnection = new ClickHouseConnection(connectionString);
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

            var values = Enumerable.Range(0, (int)count).Select(i => new object[] { (long)i });
            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(values);
            stopwatch.Stop();

            // Verify we've written expected number of rows
            //Assert.AreEqual(count, bulkCopyInterface.RowsWritten);
            //Assert.AreEqual(count, Convert.ToInt32(await targetConnection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}")));

            // Clear table after benchmark
            await targetConnection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");

            var rps = (long)count * 1000 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"{count} rows written at {rps:#0.} rows/s");
            return new BenchmarkResult { Duration = stopwatch.Elapsed, RowsCount = count, DataSize = count * sizeof(long) };
        }
    }
}
