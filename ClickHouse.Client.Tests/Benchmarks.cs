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
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT number FROM system.numbers LIMIT {count}";
            using var reader = await command.ExecuteReaderAsync();

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
            const string targetDatabase = "default";
            const string targetTable = "discard";

            var stopwatch = new Stopwatch();
            using var sourceConnection = TestUtilities.GetTestClickHouseConnection(Driver, true);
            using var targetConnection = TestUtilities.GetTestClickHouseConnection(Driver, true);
            targetConnection.ChangeDatabase(targetDatabase);

            using var tcommand = targetConnection.CreateCommand();
            tcommand.CommandText = $"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Null";
            tcommand.ExecuteNonQuery(); // Create target table

            using var scommand = sourceConnection.CreateCommand();

            scommand.CommandText = $"SELECT number FROM system.numbers LIMIT {count}";
            using var reader = await scommand.ExecuteReaderAsync();

            using var bulkCopyInterface = new ClickHouseBulkCopy(targetConnection)
            {
                DestinationTableName = targetTable,
                BatchSize = 100000
            };

            stopwatch.Start();
            await bulkCopyInterface.WriteToServerAsync(reader);
            stopwatch.Stop();

            var rps = (double)count / stopwatch.ElapsedMilliseconds * 1000;
            Assert.Pass($"{rps:#0.} rows/s");
        }
    }
}
