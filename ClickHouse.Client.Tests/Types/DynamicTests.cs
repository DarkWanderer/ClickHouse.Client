using System;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Tests.Attributes;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Tests.Types;

public class DynamicTests : AbstractConnectionTestFixture
{
    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCase("null", ExpectedResult = typeof(DBNull))]
    [TestCase("1", ExpectedResult = typeof(long))]
    [TestCase("1000", ExpectedResult = typeof(long))]
    [TestCase("100000", ExpectedResult = typeof(long))]
    [TestCase("-1", ExpectedResult = typeof(long))]
    [TestCase("1.2", ExpectedResult = typeof(double))]
    [TestCase("-1.2", ExpectedResult = typeof(double))]
    [TestCase("\"str\"", ExpectedResult = typeof(string))]
    [TestCase("\"1.2.3.4\"", ExpectedResult = typeof(string))]
    [TestCase("\"2025-04-10 20:12:07.363000000\"", ExpectedResult = typeof(DateTime))]
    [TestCase("\"00000000-0000-0000-0000-000000000000\"", ExpectedResult = typeof(string))]
    [TestCase("[1]", ExpectedResult = typeof(long?[]))]
    [TestCase("[1.2]", ExpectedResult = typeof(double?[]))]
    [TestCase("[\"str\"]", ExpectedResult = typeof(string[]))]
    public async Task<Type> ShouldConvertToClickHouseTypeAsync(object value)
    {
        using var reader =
            (ClickHouseDataReader) await connection.ExecuteReaderAsync(
                $$"""
                  WITH t as (SELECT '{"value": {{value}}}'::JSON as json) 
                  select json.value from t
                  """);

        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        ClassicAssert.IsFalse(reader.Read());

        return result.GetType();
    }
}
