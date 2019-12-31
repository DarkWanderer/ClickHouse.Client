using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [NonParallelizable]
    [TestFixture(true, TestName = "BenchmarkWithCompression")]
    [TestFixture(false, TestName = "BenchmarkWithoutCompression")]
    public class Benchmarks
    {
        private ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.Binary;

        private const int Multiplier = 1; // Increase this number to run actual benchmark or profiling
        private readonly bool useCompression;

        public Benchmarks(bool useCompression)
        {
            this.useCompression = useCompression;
        }

        [Test(Description = "Select single integer column")]
        public async Task SelectSingleColumnBenchmark()
        {
            var stopwatch = new Stopwatch();

            const int count = 50000 * Multiplier;
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver, useCompression);
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            int counter = 0;
            Assert.IsTrue(reader.HasRows);
            stopwatch.Start();
            while (reader.Read())
                counter++;
            stopwatch.Stop();
            Assert.IsFalse(reader.HasRows);
            Assert.AreEqual(count, counter);

            var rps = (long)count * 1000 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"{rps:#0.} rows/s");
        }

        [Test(Description = "Write single column with large number of values")]
        public async Task BulkCopyBenchmark()
        {
            const int count = 20000 * Multiplier;
            string targetDatabase = "temp";
            string targetTable = $"{targetDatabase}.bulk_insert_test";

            var stopwatch = new Stopwatch();

            // Create database and table for benchmark
            using var targetConnection = TestUtilities.GetTestClickHouseConnection(Driver, useCompression);
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

            var values = Enumerable.Range(0, count).Select(i => new object[] { (long)i });
            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(values);
            stopwatch.Stop();

            // Verify we've written expected number of rows
            Assert.AreEqual(count, bulkCopyInterface.RowsWritten);
            Assert.AreEqual(count, Convert.ToInt32(await targetConnection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}")));

            // Clear table after benchmark
            await targetConnection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");

            var rps = (long)count * 1000 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"{rps:#0.} rows/s");
        }
    }
}
