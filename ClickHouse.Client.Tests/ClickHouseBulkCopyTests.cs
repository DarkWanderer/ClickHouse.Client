using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ClickHouseBulkCopyTests
    {
        private ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.Binary;

        [Test]
        public async Task ShouldBulkCopyData()
        {
            const int count = 500000; // Increase this number to use in profiling
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
