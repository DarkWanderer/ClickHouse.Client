using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(ClickHouseConnectionDriver.Binary, true)]
    [TestFixture(ClickHouseConnectionDriver.JSON, true)]
    [TestFixture(ClickHouseConnectionDriver.TSV, true)]
    [TestFixture(ClickHouseConnectionDriver.Binary, false)]
    [TestFixture(ClickHouseConnectionDriver.JSON, false)]
    [TestFixture(ClickHouseConnectionDriver.TSV, false)]
    public class SqlSelectTests
    {
        private readonly ClickHouseConnectionDriver driver;
        private readonly DbConnection connection;

        public SqlSelectTests(ClickHouseConnectionDriver driver, bool useCompression)
        {
            this.driver = driver;
            connection = TestUtilities.GetTestClickHouseConnection(driver, useCompression);
        }

        public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
            .Select(sample => new TestCaseData($"SELECT {sample.ExampleExpression}") { ExpectedResult = sample.ExampleValue });

        [Test]
        [TestCaseSource(typeof(SqlSelectTests), nameof(SimpleSelectQueries))]
        public async Task<object> ShouldExecuteSimpleSelectQuery(string sql)
        {
            using var reader = await connection.ExecuteReaderAsync(sql);
            reader.AssertHasFieldCount(1);
            var result = reader.GetEnsureSingleRow().Single();

            if (driver == ClickHouseConnectionDriver.JSON)
                Assert.Inconclusive("TODO: fix types matching for JSON");

            return result;
        }

        [Test]
        public async Task ShouldSelectMultipleColumns()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 as a, 2 as b, 3 as c");

            reader.AssertHasFieldCount(3);
            CollectionAssert.AreEqual(new[] { "a", "b", "c" }, reader.GetFieldNames());
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, reader.GetEnsureSingleRow());
        }

        [Test]
        public async Task ShouldSelectEmptyDataset()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 LIMIT 0");

            reader.AssertHasFieldCount(1);
            //Assert.IsFalse(reader.HasRows);
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

            using var reader = await connection.ExecuteReaderAsync(sql);
            Assert.AreEqual(types.Length, reader.FieldCount);

            var data = reader.GetEnsureSingleRow();
            Assert.AreEqual(Enumerable.Repeat(55.0d, data.Length), data);
        }

        [Test]
        public async Task ShouldSelectSingleColumnRange()
        {
            const int count = 100;
            using var reader = await connection.ExecuteReaderAsync($"SELECT number FROM system.numbers LIMIT {count}");

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            reader.AssertHasFieldCount(1);
            Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));

            while (reader.Read())
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32

            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        [NonParallelizable]
        public async Task ShouldSelectNestedDataType()
        {
            await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test");
            await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.nested");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.nested(nested_v Nested (int16_v Int16, uint32_v UInt32, dtime_v DateTime, string_v String)) ENGINE = Memory");

            using var reader = await connection.ExecuteReaderAsync("SELECT nested_v.int16_v, nested_v.uint32_v, nested_v.dtime_v, nested_v.string_v FROM test.nested");
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(3)";
            var task = command.ExecuteScalarAsync();
            await Task.Delay(50);
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
