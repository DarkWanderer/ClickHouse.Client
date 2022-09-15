using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(true)]
    [TestFixture(false)]
    public class SqlSimpleSelectTests : IDisposable
    {
        private readonly ClickHouseConnection connection;

        public SqlSimpleSelectTests(bool useCompression)
        {
            connection = TestUtilities.GetTestClickHouseConnection(useCompression);
        }

        public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
            .Select(sample => new TestCaseData($"SELECT {sample.ExampleExpression}") { ExpectedResult = sample.ExampleValue });

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(SqlSimpleSelectTests), nameof(SimpleSelectQueries))]
        public async Task<object> ShouldExecuteSimpleSelectQuery(string sql)
        {
            using var reader = await connection.ExecuteReaderAsync(sql);
            reader.AssertHasFieldCount(1);
            var result = reader.GetEnsureSingleRow().Single();

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
        [TestCase("Добрый день")]
        [TestCase("¿Qué tal?")]
        [TestCase("你好")]
        [TestCase("こんにちは")]
        [TestCase("⌬⏣")]
        [Parallelizable]
        public async Task ShouldSelectUnicode(string input)
        {
            using var reader = await connection.ExecuteReaderAsync($"SELECT '{input}'");

            reader.AssertHasFieldCount(1);
            var result = reader.GetEnsureSingleRow().Single();
            Assert.AreEqual(input, result);
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
        public async Task DateTimeSelectShouldHaveCorrectTimezone()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT toDateTime(1577836800, 'Asia/Sakhalin')");

            reader.AssertHasFieldCount(1);
            var datetime = (DateTime)reader.GetEnsureSingleRow().Single();
            if (datetime.Kind == DateTimeKind.Utc)
            {
                Assert.AreEqual(new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc), datetime.ToUniversalTime());
            }
            else
            {
                Assert.AreEqual(new DateTime(2020, 01, 01, 11, 0, 0, DateTimeKind.Unspecified), datetime);
                Assert.AreEqual(DateTimeKind.Unspecified, datetime.Kind);
            }
        }

        [Test]
        [RequiredFeature(Feature.DateTime64)]
        public async Task DateTime64SelectShouldHaveCorrectTimezone()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT toDateTime64(1577836800, 3, 'Asia/Sakhalin')");

            reader.AssertHasFieldCount(1);
            var datetime = (DateTime)reader.GetEnsureSingleRow().Single();
            if (datetime.Kind == DateTimeKind.Utc)
            {
                Assert.AreEqual(new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc), datetime.ToUniversalTime());
            }
            else
            {
                Assert.AreEqual(new DateTime(2020, 01, 01, 11, 0, 0, DateTimeKind.Unspecified), datetime);
                Assert.AreEqual(DateTimeKind.Unspecified, datetime.Kind);
            }
        }

        [Test]
        public async Task DateTimeOffsetShouldProduceCorrectOffset()
        {
            using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDateTime(1577836800, 'Asia/Sakhalin')");
            reader.AssertHasFieldCount(1);
            Assert.IsTrue(reader.Read());
            var dto = reader.GetDateTimeOffset(0);
            Assert.AreEqual(TimeSpan.FromHours(11), dto.Offset);
            Assert.AreEqual(new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc), dto.UtcDateTime);
        }

        [Test]
        public async Task ShouldSelectNumericTypes()
        {
            var types = TypeConverter.RegisteredTypes
                .Where(dt => dt.Contains("Int") || dt.Contains("Float"))
                .Where(dt => !dt.Contains("128") || TestUtilities.SupportedFeatures.HasFlag(Feature.WideTypes))
                .Where(dt => !dt.Contains("256") || TestUtilities.SupportedFeatures.HasFlag(Feature.WideTypes))
                .Select(dt => $"to{dt}(55)")
                .ToArray();

            var sql = $"select {string.Join(',', types)}";

            using var reader = await connection.ExecuteReaderAsync(sql);
            Assert.AreEqual(types.Length, reader.FieldCount);

            var data = reader.GetEnsureSingleRow();
            Assert.That(data, Is.All.EqualTo(55).Or.EqualTo(new BigInteger(55)));
        }

        [Test]
        public async Task ShouldSelectSingleColumnRange()
        {
            const int count = 100;
            using var reader = await connection.ExecuteReaderAsync($"SELECT toInt16(number) FROM system.numbers LIMIT {count}");

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            reader.AssertHasFieldCount(1);
            Assert.AreEqual(typeof(short), reader.GetFieldType(0));

            while (reader.Read())
                results.Add(reader.GetInt16(0)); // Intentional conversion to int32

            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(3)";
            var task = command.ExecuteScalarAsync();
            await Task.Delay(50).ConfigureAwait(false);
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

        [Test]
        public async Task ShouldSelectSimpleAggregateFunction()
        {
            var result = await connection.ExecuteScalarAsync("SELECT CAST(1,'SimpleAggregateFunction(anyLast, Nullable(Float64))')");
            Assert.AreEqual(1, result);
        }

        [Test]
        [RequiredFeature(Feature.Stats)]
        public async Task ShouldGetQueryStats()
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT * FROM system.numbers LIMIT 100";
            using var reader = await command.ExecuteReaderAsync();
            Assert.AreEqual(new QueryStats(100, 800, 0, 0, 0), command.QueryStats);
        }

        public void Dispose() => connection?.Dispose();
    }
}
