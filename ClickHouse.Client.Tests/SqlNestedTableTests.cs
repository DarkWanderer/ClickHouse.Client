using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class SqlNestedTableTests
    {
        private const string Database = "test";
        private const string Table = "test.nested";

        private readonly ClickHouseConnection connection;

        public SqlNestedTableTests()
        {
            connection = TestUtilities.GetTestClickHouseConnection(true);
        }

        [SetUp]
        public async Task Setup()
        {
            await connection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS {Database}");
            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {Table}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {Table}(id UInt32, params Nested (param_id UInt8, param_val String)) ENGINE = Memory");
        }

        [Test]
        public async Task ShouldInsertIntoNestedTable()
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
            Assert.IsTrue(reader.Read());
            var values = reader.GetFieldValues();
            Assert.AreEqual(2, values[0]);
            CollectionAssert.AreEquivalent(new[] { 4, 5, 6 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v4", "v5", "v6" }, values[2] as IEnumerable);
        }
    }
}
