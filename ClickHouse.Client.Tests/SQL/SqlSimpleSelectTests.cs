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
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetFieldNames(), Is.EqualTo(new[] { "a", "b", "c" }).AsCollection);
            Assert.That(reader.GetEnsureSingleRow(), Is.EqualTo(new[] { 1, 2, 3 }).AsCollection);
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(dt.Year, Is.EqualTo(2020));
            Assert.That(dt.Month, Is.EqualTo(1));
            Assert.That(dt.Day, Is.EqualTo(1));
            Assert.That(dt.Hour, Is.EqualTo(0));
            Assert.That(dt.Minute, Is.EqualTo(0));
            Assert.That(dt.Second, Is.EqualTo(0));
        });

        if (dto.Offset == TimeSpan.Zero)
        {
            Assert.That(dt.Kind, Is.EqualTo(DateTimeKind.Utc));
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
        Assert.That(reader.FieldCount, Is.EqualTo(types.Length));

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
        Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(short)));

        while (reader.Read())
            results.Add(reader.GetInt16(0)); // Intentional conversion

        Assert.That(results, Is.EqualTo(Enumerable.Range(0, count)).AsCollection);
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
        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    [RequiredFeature(Feature.Stats)]
    public async Task ShouldGetBaseQueryStats()
    {
        var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM system.numbers LIMIT 100";
        using var reader = await command.ExecuteReaderAsync();
        var stats = command.QueryStats;
        Assert.Multiple(() =>
        {
            Assert.That(stats.ReadRows, Is.EqualTo(100));
            Assert.That(stats.ReadBytes, Is.EqualTo(800));
            Assert.That(stats.WrittenRows, Is.EqualTo(0));
            Assert.That(stats.WrittenBytes, Is.EqualTo(0));
        });
    }

    [Test]
    [FromVersion(23, 8)]
    public async Task ShouldGetElapsedQueryStats()
    {
        var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM system.numbers LIMIT 100";
        using var reader = await command.ExecuteReaderAsync();
        var stats = command.QueryStats;
        Assert.Greater(stats.ElapsedNs, 0);
    }

    [Test]
    [FromVersion(23, 7)]
    public async Task ShouldGetResultQueryStats()
    {
        var command = connection.CreateCommand();
        command.CustomSettings.Add("wait_end_of_query", 1);
        command.CommandText = "SELECT * FROM system.numbers LIMIT 100";
        using var reader = await command.ExecuteReaderAsync();
        var stats = command.QueryStats;
        Assert.Multiple(() =>
        {
            Assert.That(stats.ResultRows, Is.EqualTo(100));
            Assert.That(stats.ResultBytes, Is.EqualTo(928));
        });
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
    public async Task ShouldGetValueDecimal()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDecimal32(1234.56, 3)");
        Assert.IsTrue(reader.Read());
        Assert.That(reader.GetDecimal(0), Is.EqualTo(1234.56m));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    [FromVersion(23, 6)]
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
