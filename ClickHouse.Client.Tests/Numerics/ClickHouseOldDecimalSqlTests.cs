using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Numerics;

public class ClickHouseOldDecimalSqlTests
{
    private readonly ClickHouseConnection connection;

    public ClickHouseOldDecimalSqlTests()
    {
        connection = TestUtilities.GetTestClickHouseConnection(customDecimals: false);
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
            var values = Enumerable.Range(0, 10).Select(i => $"1{new string('0', i)}").Select(Convert.ToDecimal).ToList();

            return from typeName in DecimalTypes
                   from v in values
                   let type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default)
                   where v < type.MaxValue && v > type.MinValue
                   select new TestCaseData(v, $"SELECT CAST('{v}', '{type}')");
        }
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseOldDecimalSqlTests), nameof(DecimalTestCases))]
    public async Task Select(decimal expected, string sql)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        Assert.IsInstanceOf<decimal>(result);
        Assert.That(result, Is.EqualTo(expected));
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
    }
}
