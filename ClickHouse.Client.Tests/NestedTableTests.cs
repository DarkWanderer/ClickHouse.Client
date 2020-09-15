using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class NestedTableTests
    {
        private readonly string Table = $"test.nested";
        private readonly ClickHouseConnection connection;

        public NestedTableTests()
        {
            connection = TestUtilities.GetTestClickHouseConnection(true);
        }

        [SetUp]
        public async Task Setup()
        {
            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {Table}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {Table}(id UInt32, params Nested (param_id UInt8, param_val String)) ENGINE = Memory");
        }

        [Test]
        public async Task ShouldInsertIntoNestedTableViaBulk()
        {
            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = Table,
            };
            var row1 = new object[] { 1, new[] { 1, 2, 3 }, new[] { "v1", "v2", "v3" } };
            var row2 = new object[] { 2, new[] { 4, 5, 6 }, new[] { "v4", "v5", "v6" } };

            await bulkCopy.WriteToServerAsync(new[] { row1, row2 }, CancellationToken.None);
            using var reader = await connection.ExecuteReaderAsync("SELECT * FROM test.nested ORDER BY id ASC");

            Assert.IsTrue(reader.Read());
            var values = reader.GetFieldValues();
            Assert.AreEqual(1, values[0]);
            CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v1", "v2", "v3" }, values[2] as IEnumerable);

            Assert.IsTrue(reader.Read());
            values = reader.GetFieldValues();
            Assert.AreEqual(2, values[0]);
            CollectionAssert.AreEquivalent(new[] { 4, 5, 6 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v4", "v5", "v6" }, values[2] as IEnumerable);
        }

        [Test]
        public async Task ShouldInsertIntoNestedTableViaParameters()
        {
            var row = new object[] { 1, new[] { 1, 2, 3 } };
            using var command = connection.CreateCommand();
            command.CommandText = "INSERT INTO test.nested VALUES ({id:UInt32}, {key:Array(UInt8)}, {val:Array(String)})";
            command.AddParameter("id", 1);
            command.AddParameter("key", new[] { 1, 2, 3 });
            command.AddParameter("val", new[] { "v1", "v2", "v3" });
            await command.ExecuteNonQueryAsync();

            using var reader = await connection.ExecuteReaderAsync("SELECT * FROM test.nested ORDER BY id ASC");
            Assert.IsTrue(reader.Read());
            var values = reader.GetFieldValues();
            Assert.AreEqual(1, values[0]);
            CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v1", "v2", "v3" }, values[2] as IEnumerable);
        }
    }
}
