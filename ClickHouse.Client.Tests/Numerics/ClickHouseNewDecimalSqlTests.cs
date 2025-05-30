﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Numerics;

[Category("ClickHouseDecimal")]
public class ClickHouseNewDecimalSqlTests
{
    private readonly ClickHouseConnection connection;

    public ClickHouseNewDecimalSqlTests()
    {
        connection = TestUtilities.GetTestClickHouseConnection(customDecimals: true);
    }

    public static IEnumerable<string> DecimalTypes
    {
        get
        {
            yield return "Decimal32(3)";
            yield return "Decimal64(3)";
            yield return "Decimal128(3)";
            if (TestUtilities.SupportedFeatures.HasFlag(Feature.WideTypes))
            {
                yield return "Decimal256(3)";
            }
        }
    }

    public static IEnumerable<TestCaseData> DecimalTestCases
    {
        get
        {
            var values = Enumerable.Range(0, 50).Select(i => $"1{new string('0', i)}").Select(ClickHouseDecimal.Parse).ToList();

            return from typeName in DecimalTypes
                   from v in values
                   let type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default)
                   where v < type.MaxValue && v > type.MinValue
                   select new TestCaseData(v, $"SELECT CAST('{v}', '{type}')");
        }
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseNewDecimalSqlTests), nameof(DecimalTypes))]
    public async Task SelectMaxValue(string typeName)
    {
        var type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default);
        using var reader = await connection.ExecuteReaderAsync($"SELECT CAST('{type.MaxValue}', '{type}')");
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        ClassicAssert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.That(result, Is.EqualTo(type.MaxValue));
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseNewDecimalSqlTests), nameof(DecimalTypes))]
    public async Task SelectMinValue(string typeName)
    {
        var type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default);
        using var reader = await connection.ExecuteReaderAsync($"SELECT CAST('{type.MinValue}', '{type}')");
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        ClassicAssert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.That(result, Is.EqualTo(type.MinValue));
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseNewDecimalSqlTests), nameof(DecimalTestCases))]
    public async Task Select(ClickHouseDecimal expected, string sql)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        ClassicAssert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.That(result, Is.EqualTo(expected));
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
    }
}
