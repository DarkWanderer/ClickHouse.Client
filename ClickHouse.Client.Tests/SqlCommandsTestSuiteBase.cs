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

            reader.EnsureFieldCount(3);
            reader.GetEnsureSingleRow();
            CollectionAssert.AreEqual(new[] { "a", "b", "c" }, reader.GetFieldNames());
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, reader.GetFieldValues());
        }

        [Test]
        public async Task ShouldSelectNumericTypes()
        {
            var types = Enum.GetValues(typeof(ClickHouseDataType))
                .Cast<ClickHouseDataType>()
                .Select(dt => dt.ToString())
                .Where(dt => dt.Contains("Int") || dt.Contains("Float"))
                .Select(dt => $"to{dt.ToString()}(1)")
                .ToArray();
            var sql = $"select {string.Join(',', types)}";

            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync();
            Assert.AreEqual(types.Length, reader.FieldCount);

            var data = reader.GetEnsureSingleRow();
            Assert.AreEqual(Enumerable.Repeat("1", data.Length), data.Select(x => x.ToString()));
        }

        [Test]
        public async Task ShouldSelectStringTypes()
        {
            var sql = $"select 'ASD', toFixedString('ASD', 3)";

            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = await command.ExecuteReaderAsync();

            reader.EnsureFieldCount(2);
            var data = reader.GetEnsureSingleRow();

            for (int i = 0; i < reader.FieldCount; i++)
                Assert.AreEqual("ASD", data[i]);
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
            reader.EnsureFieldCount(1);

            if (Driver != ClickHouseConnectionDriver.JSON)
                Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));
            else
                Assert.AreEqual(typeof(string), reader.GetFieldType(0));

            while (reader.Read())
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32

            Assert.IsFalse(reader.HasRows);
            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        public async Task ShouldSelectArrayType()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT array(1, 2, 3, 4, 5)";
            var reader = await command.ExecuteReaderAsync();
            var data = reader.GetEnsureSingleRow().Single() as object[];
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5 }, data);
        }

        [Test]
        public async Task ShouldSelectMixedType()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT array(1, 2, 3, NULL)";
            var reader = await command.ExecuteReaderAsync();
            var data = reader.GetEnsureSingleRow().Single() as object[];
            CollectionAssert.AreEqual(new object[] { 1, 2, 3, DBNull.Value }, data);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(5)";
            var task = command.ExecuteScalarAsync();
            command.Cancel();

            try
            {
                await task;
                Assert.Fail("Expected to receive TaskCancelledException from task");
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
