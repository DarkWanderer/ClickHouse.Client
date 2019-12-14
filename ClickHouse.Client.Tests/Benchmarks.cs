using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [NonParallelizable]
    public class Benchmarks
    {
        private ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.Binary;

        private const int Multiplier = 1; // Increase this number to run actual benchmark or profiling

        [Test(Description = "Select single integer column")]
        public async Task SelectSingleColumnBenchmark()
        {
            var stopwatch = new Stopwatch();

            const int count = 100000 * Multiplier;
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            int counter = 0;
            Assert.IsTrue(reader.HasRows);
            stopwatch.Start();
            while (reader.Read())
                counter++;
            stopwatch.Stop();
            Assert.AreEqual(count, counter);

            var rps = (double)count / stopwatch.ElapsedMilliseconds * 1000;
            Assert.Pass($"{rps:#0.} rows/s");
        }

        [Test]
        public async Task BulkCopyBenchmark()
        {
            const int count = 20000 * Multiplier;
            string targetDatabase = "temp";
            string targetTable = $"{targetDatabase}.bulk_insert_test";

            var stopwatch = new Stopwatch();
            using var sourceConnection = TestUtilities.GetTestClickHouseConnection(Driver, true);
            using var targetConnection = TestUtilities.GetTestClickHouseConnection(Driver, true);
            targetConnection.ChangeDatabase(targetDatabase);

            await targetConnection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS {targetDatabase}");
            await targetConnection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Log");

            using var reader = await sourceConnection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            using var bulkCopyInterface = new ClickHouseBulkCopy(targetConnection)
            {
                DestinationTableName = targetTable,
                BatchSize = 100000
            };

            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(reader);
            stopwatch.Stop();

            var rps = (double)count / stopwatch.ElapsedMilliseconds * 1000;
            Assert.AreEqual(count, bulkCopyInterface.RowsWritten);
            Assert.Pass($"{rps:#0.} rows/s");
        }
    }
}
