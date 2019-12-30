using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Types;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(ClickHouseConnectionDriver.Binary)]
    [TestFixture(ClickHouseConnectionDriver.JSON)]
    [TestFixture(ClickHouseConnectionDriver.TSV)]
    public class SqlSelectTests
    {
        private readonly ClickHouseConnectionDriver driver;

        public SqlSelectTests(ClickHouseConnectionDriver driver)
        {
            this.driver = driver;
        }

        public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
            .Select(sample => new TestCaseData($"SELECT {sample.ExampleExpression}") { ExpectedResult = sample.ExampleValue });

        [Test]
        [TestCaseSource(typeof(SqlSelectTests), nameof(SimpleSelectQueries))]
        public async Task<object> ShouldExecuteSimpleSelectQuery(string sql)
        {
            if (driver == ClickHouseConnectionDriver.JSON && sql.Contains("tuple"))
                Assert.Ignore("JSON does not correctly handle Tuple");

            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var reader = await connection.ExecuteReaderAsync(sql);
            reader.AssertHasFieldCount(1);
            var result = reader.GetEnsureSingleRow().Single();
            return result;
        }

        [Test]
        public async Task ShouldSelectMultipleColumns()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 as a, 2 as b, 3 as c");

            reader.AssertHasFieldCount(3);
            CollectionAssert.AreEqual(new[] { "a", "b", "c" }, reader.GetFieldNames());
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, reader.GetEnsureSingleRow());
        }

        [Test]
        public async Task ShouldSelectEmptyDataset()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 LIMIT 0");

            reader.AssertHasFieldCount(1);
            Assert.IsFalse(reader.HasRows);
            Assert.IsFalse(reader.Read());
        }

        [Test]
        public async Task ShouldSelectNumericTypes()
        {
            var types = Enum.GetValues(typeof(ClickHouseTypeCode))
                .Cast<ClickHouseTypeCode>()
                .Select(dt => dt.ToString())
                .Where(dt => dt.Contains("Int") || dt.Contains("Float"))
                .Select(dt => $"to{dt.ToString()}(55)")
                .ToArray();
            var sql = $"select {string.Join(',', types)}";

            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var reader = await connection.ExecuteReaderAsync(sql);
            Assert.AreEqual(types.Length, reader.FieldCount);

            var data = reader.GetEnsureSingleRow();
            Assert.AreEqual(Enumerable.Repeat(55.0d, data.Length), data);
        }

        [Test]
        public async Task ShouldSelectSingleColumnRange()
        {
            const int count = 100;
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            reader.AssertHasFieldCount(1);
            Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));

            while (reader.Read())
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32

            Assert.IsFalse(reader.HasRows);
            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        [NonParallelizable]
        public async Task ShouldSelectNestedDataType()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test");
            await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.nested");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.nested(nested_v Nested (int16_v Int16, uint32_v UInt32, dtime_v DateTime, string_v String)) ENGINE = Memory");

            using var reader = await connection.ExecuteReaderAsync("SELECT nested_v.int16_v, nested_v.uint32_v, nested_v.dtime_v, nested_v.string_v FROM test.nested");
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(3)";
            var task = command.ExecuteScalarAsync();
            await Task.Delay(100);
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
}
