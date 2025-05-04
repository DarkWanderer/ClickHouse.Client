using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Tests.Types;

public class DynamicTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
        .Where(s => ShouldBeSupportedInJson(s.ClickHouseType))
        .Select(sample => GetTestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

    [Test]
    [TestCaseSource(typeof(DynamicTests), nameof(SimpleSelectQueries))]
    public async Task ShouldMatchFrameworkType(string valueSql, Type frameworkType)
    {
        using var reader =
            (ClickHouseDataReader) await connection.ExecuteReaderAsync(
                $"select json.value from (select toJSONString(map('value', {valueSql}))::JSON as json)");

        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        Assert.That(result.GetType(), Is.EqualTo(frameworkType));
        ClassicAssert.IsFalse(reader.Read());
    }

    private static TestCaseData GetTestCaseData(string exampleExpression, string clickHouseType, object exampleValue)
    {
        if (clickHouseType.StartsWith("Date"))
        {
            return new TestCaseData(exampleExpression, typeof(DateTime));
        }

        if (clickHouseType.StartsWith("Int") || clickHouseType.StartsWith("UInt"))
        {
            return new TestCaseData(exampleExpression, typeof(long));
        }

        if (clickHouseType.StartsWith("FixedString"))
        {
            return new TestCaseData(exampleExpression, typeof(string));
        }

        if (clickHouseType.StartsWith("Float"))
        {
            var floatRemainder =
                exampleValue switch
                {
                    double @double => @double % 10,
                    float @float => @float % 10,
                    _ => throw new ArgumentException($"{exampleValue.GetType().Name} not supported in for Float")
                };
            return new TestCaseData(
                exampleExpression,
                floatRemainder is 0
                    ? typeof(long)
                    : typeof(double));
        }

        switch (clickHouseType)
        {
            case "Array(Int32)" or "Array(Nullable(Int32))":
                return new TestCaseData(exampleExpression, typeof(long?[]));
            case "Array(String)":
                return new TestCaseData(exampleExpression, typeof(string[]));
            case "IPv4" or "IPv6" or "String" or "UUID":
                return new TestCaseData(exampleExpression, typeof(string));
            case "Nothing":
                return new TestCaseData(exampleExpression, typeof(DBNull));
            case "Bool":
                return new TestCaseData(exampleExpression, typeof(bool));
        }

        throw new ArgumentException($"{clickHouseType} not supported");
    }

    private static bool ShouldBeSupportedInJson(string clickHouseType)
    {
        if (clickHouseType.Contains("Decimal") ||
            clickHouseType.Contains("Enum") ||
            clickHouseType.Contains("FixedString") ||
            clickHouseType.Contains("LowCardinality") ||
            clickHouseType.Contains("Map") ||
            clickHouseType.Contains("Nested") ||
            clickHouseType.Contains("Nullable") ||
            clickHouseType.Contains("Tuple") ||
            clickHouseType.Contains("Variant"))
        {
            return false;
        }

        switch (clickHouseType)
        {
            case "Date32":
            case "Int128":
            case "Int256":
            case "Json":
            case "Point":
            case "Ring":
            case "UInt128":
            case "UInt256":
                return false;
            default:
                return true;
        }
    }
}
