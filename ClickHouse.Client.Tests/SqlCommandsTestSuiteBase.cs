using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Types;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    public abstract class SqlCommandsTestSuiteBase
    {
        protected abstract ClickHouseConnectionDriver Driver { get; }

        [Test]
        public async Task ShouldExecuteScalar()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            Assert.AreEqual(1, await command.ExecuteScalarAsync());
        }

        [Test]
        public async Task ShouldSelectMultipleColumns()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 as a, 2 as b, 3 as c";
            using var reader = await command.ExecuteReaderAsync();
            Assert.AreEqual(3, reader.FieldCount);
            Assert.IsTrue(reader.HasRows);
            Assert.IsTrue(reader.Read());

            CollectionAssert.AreEqual(new[] { "a", "b", "c" }, Enumerable.Range(0, 3).Select(reader.GetName));
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, Enumerable.Range(0, 3).Select(reader.GetValue));
            Assert.IsFalse(reader.HasRows);
        }

        [Test]
        public async Task ShouldSelectMultipleTypes()
        {
            var types = Enum.GetValues(typeof(DataType))
                .Cast<DataType>()
                .Select(dt => dt.ToString())
                .Where(dt => dt.Contains("Int") || dt.Contains("Float"))
                .Select(dt => $"to{dt.ToString()}(1)")
                .ToArray();
            var sql = $"select {string.Join(',', types)}";

            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = await command.ExecuteReaderAsync();
            Assert.IsTrue(reader.HasRows);
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(types.Length, reader.FieldCount);

            for (int i = 0; i < reader.FieldCount; i++)
                Assert.AreEqual("1", reader.GetValue(i).ToString());

            Assert.IsFalse(reader.HasRows);
            Assert.IsFalse(reader.Read());
        }

        [Test]
        public async Task ShouldSelectSingleColumnRange()
        {
            const int count = 100;
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT number FROM system.numbers LIMIT {count}";
            using var reader = await command.ExecuteReaderAsync();

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            Assert.AreEqual(1, reader.FieldCount);

            if (Driver != ClickHouseConnectionDriver.JSON)
                Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));

            while (reader.Read())
            {
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32
            }

            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT number FROM system.numbers LIMIT 100000000";
            var task = command.ExecuteScalarAsync();
            command.Cancel();

            try
            {
                await task;
            }
            catch (TaskCanceledException)
            {
                // Correct
            }
        }
    }

    public class JsonDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.JSON;
    }
    public class BinaryDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.Binary;
    }
    public class TsvDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.TSV;
    }
}
