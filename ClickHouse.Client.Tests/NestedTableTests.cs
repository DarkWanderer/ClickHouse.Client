using System.Collections;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class NestedTableTests : AbstractConnectionTestFixture
    {
        private readonly string Table = $"test.nested";
        private readonly string Table2 = $"test.nested2";
        private readonly string Table3 = $"test.nested3";

        [SetUp]
        public async Task Setup()
        {
            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {Table}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {Table}(id UInt32, params Nested (param_id UInt8, param_val String)) ENGINE = Memory");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {Table2}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {Table2}(id UInt32, params Nested (param_id UInt8, param_val String, sub_nested Nested (param_id UInt8, param_val String))) ENGINE = Memory");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {Table3}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {Table3}(id UInt32, params Nested (param_id UInt8, param_val String, sub_nested Nested (param_id UInt8, param_val String, sub_sub_nested Nested(param_id UInt8, param_val String)))) ENGINE = Memory");
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
            using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {Table} ORDER BY id ASC");

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
        public async Task ShouldInsertIntoSubNestedTableViaBulk()
        {
            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = Table2,
            };
            var row1 = new object[] { 1, new[] { 1 }, new[] { "v1" }, new[] { new[] { new object[] { 1, "v1" }, new object[] { 2, "v2" }, new object[] { 3, "v3" } } } };
            var row2 = new object[] { 2, new[] { 4,8 }, new[] { "v4","v8" }, new[] { new[] { new object[] { 4, "v4" }, new object[] { 5, "v5" }, new object[] { 6, "v6" } }, new[] { new object[] { 9, "v9" } } } };

            await bulkCopy.WriteToServerAsync(new[] { row1, row2 }, CancellationToken.None);
            using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {Table2} ORDER BY id ASC");

            Assert.IsTrue(reader.Read());
            var values = reader.GetFieldValues();
            Assert.AreEqual(1, values[0]);
            CollectionAssert.AreEquivalent(new[] { 1 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v1" }, values[2] as IEnumerable);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[0], 1);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[1], "v1");
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[1])[0], 2);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[1])[1], "v2");
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[2])[0], 3);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[2])[1], "v3");
            

            Assert.IsTrue(reader.Read());
            values = reader.GetFieldValues();
            Assert.AreEqual(2, values[0]);
            CollectionAssert.AreEquivalent(new[] { 4, 8 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v4", "v8" }, values[2] as IEnumerable);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[0], 4);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[1], "v4");
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[1])[0], 5);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[1])[1], "v5");
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[2])[0], 6);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[2])[1], "v6");
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[1])[0])[0], 9);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[1])[0])[1], "v9");
        }

        [Test]
        public async Task ShouldInsertIntoSubSubNestedTableViaBulk()
        {
            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = Table3,
            };
            var row1 = new object[] { 1, new[] { 1 }, new[] { "v1" }, new[] { new[] { new object[] { 1, "v1" , new[] { new object[] { 2, "sub_v2" } } } } } };
            

            await bulkCopy.WriteToServerAsync(new[] { row1/*, row2*/ }, CancellationToken.None);
            using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {Table3} ORDER BY id ASC");

            Assert.IsTrue(reader.Read());
            var values = reader.GetFieldValues();
            Assert.AreEqual(1, values[0]);
            CollectionAssert.AreEquivalent(new[] { 1 }, values[1] as IEnumerable);
            CollectionAssert.AreEquivalent(new[] { "v1" }, values[2] as IEnumerable);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[0], 1);
            Assert.AreEqual(((ITuple)((object[])((object[])values[3])[0])[0])[1], "v1");
            Assert.AreEqual(((ITuple)((object[])((ITuple)((object[])((object[])values[3])[0])[0])[2])[0])[0], 2);
            Assert.AreEqual(((ITuple)((object[])((ITuple)((object[])((object[])values[3])[0])[0])[2])[0])[1], "sub_v2");
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
