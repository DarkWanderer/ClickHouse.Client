using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Tests.Attributes;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.SQL;

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

    public static IEnumerable<TestCaseData> SimpleSelectTypes => TestUtilities.GetDataTypeSamples()
        .Select(sample => new TestCaseData(sample.ClickHouseType));

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
    [Combinatorial]
    public async Task DateTimeSelectShouldReturnInOriginalTimeZone(
        [Values("Asia/Sakhalin", "Etc/UTC", "Etc/GMT", "Etc/Universal")] string timezone,
        [Values("DateTime", "DateTime64")] string type
        )
    {
        var precision = type.Contains("64") ? "3, " : "";
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT to{type}('2020-01-01 00:00:00', {precision} '{timezone}')");

        reader.AssertHasFieldCount(1);
        Assert.IsTrue(reader.Read());
        var dt = reader.GetDateTime(0);
        var dto = reader.GetDateTimeOffset(0);
        Assert.IsFalse(reader.Read());

        Assert.AreEqual(2020, dt.Year);
        Assert.AreEqual(1, dt.Month);
        Assert.AreEqual(1, dt.Day);
        Assert.AreEqual(0, dt.Hour);
        Assert.AreEqual(0, dt.Minute);
        Assert.AreEqual(0, dt.Second);

        if (dto.Offset == TimeSpan.Zero)
        {
            Assert.AreEqual(DateTimeKind.Utc, dt.Kind);
        }
    }

    [Test]
    [TestCase("Asia/Sakhalin", ExpectedResult = 11.0)]
    [TestCase("Europe/Moscow", ExpectedResult = 3.0)]
    [TestCase("Europe/London", ExpectedResult = 0.0)]
    [TestCase("Etc/UTC", ExpectedResult = 0.0)]
    [TestCase("America/New_York", ExpectedResult = -5.0)]
    public async Task<double> DateTimeOffsetShouldProduceCorrectOffset(string timezone)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT toDateTime('2020-01-01 00:00:00', '{timezone}')");
        reader.AssertHasFieldCount(1);
        Assert.IsTrue(reader.Read());
        var dto = reader.GetDateTimeOffset(0);
        return dto.Offset.TotalHours;
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

        var sql = $"select {string.Join(",", types)}";

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
            results.Add(reader.GetInt16(0)); // Intentional conversion

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
        var stats = command.QueryStats;
        Assert.AreEqual(stats.ReadRows, 100);
        Assert.AreEqual(stats.ReadBytes, 800);
        Assert.AreEqual(stats.WrittenRows, 0);
        Assert.AreEqual(stats.WrittenBytes, 0);
    }

    [Test]
    [FromVersion(23, 6)]
    public async Task ShouldSelectRandomizedData()
    {
        const int seed = 28081988;
        const int count = 20;
        const int columns = 50;
        using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM generateRandom(generateRandomStructure({columns}, {seed}), {seed}) LIMIT {count};");
        reader.AssertHasFieldCount(columns);
        while (await reader.ReadAsync()) ;
    }

    [Test]
    [FromVersion(23, 6)]
    [Parallelizable]
    [TestCaseSource(typeof(SqlSimpleSelectTests), nameof(SimpleSelectTypes))]
    public async Task ShouldExecuteRandomDataSelectQuery(string type)
    {
        if (type.StartsWith("Nested") || type == "Nothing" || type.StartsWith("Variant"))
            Assert.Ignore($"Type {type} not supported by generateRandom");

        using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM generateRandom('value {type.Replace("'", "\\'")}', 10, 10, 10) LIMIT 100");
        reader.AssertHasFieldCount(1);
    }

    public void Dispose() => connection?.Dispose();
}
