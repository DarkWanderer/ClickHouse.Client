using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

[Parallelizable]
public class SqlClickHouseDecimalTest
{
    private readonly ClickHouseConnection connection;

    public SqlClickHouseDecimalTest()
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

    public static IEnumerable<TestCaseData> DecimalTestCases { 
        get
        {
            var values = Enumerable.Range(0, 50).Select(i => $"1{new string('0', i)}").Select(ClickHouseDecimal.Parse).ToList();

            return from typeName in DecimalTypes
                   from v in values
                   let type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default)
                   where v < type.MaxValue && v > type.MinValue
                   select new TestCaseData(v, $"SELECT to{typeName}('{v}', {type.Scale})");
        } 
    }

    [Test]
    [TestCaseSource(typeof(SqlClickHouseDecimalTest), nameof(DecimalTypes))]
    [Parallelizable]
    public async Task SelectMaxValue(string typeName)
    {
        var type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default);
        using var reader = await connection.ExecuteReaderAsync($"SELECT to{typeName}('{type.MaxValue}', {type.Scale})");
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        Assert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.AreEqual(type.MaxValue, result);
    }

    [Test]
    [TestCaseSource(typeof(SqlClickHouseDecimalTest), nameof(DecimalTypes))]
    [Parallelizable]
    public async Task SelectMinValue(string typeName)
    {
        var type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default);
        using var reader = await connection.ExecuteReaderAsync($"SELECT to{typeName}('{type.MinValue}', {type.Scale})");
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        Assert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.AreEqual(type.MinValue, result);
    }

    [Test]
    [Parallelizable]
    [TestCaseSource(typeof(SqlClickHouseDecimalTest), nameof(DecimalTestCases))]
    public async Task Select(ClickHouseDecimal expected, string sql)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        Assert.IsInstanceOf<ClickHouseDecimal>(result);
        Assert.AreEqual(expected, result);
    }
}
