namespace ClickHouse.Client.Tests.SQL;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

[Parallelizable]
[TestFixture(true)]
[TestFixture(false)]
public class SqlParameterizedSelectWithFormDataTests
{
    private readonly ClickHouseConnection connection;

    public SqlParameterizedSelectWithFormDataTests(bool useCompression)
    {
        connection = TestUtilities.GetTestClickHouseConnection(useCompression);
        connection.SetFormDataParameters(true);
        connection.Open();
    }

    public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
        // DB::Exception: There are no UInt128 literals in SQL
        .Where(sample => !sample.ClickHouseType.Contains("UUID") || TestUtilities.SupportedFeatures.HasFlag(Feature.UUIDParameters))
        // DB::Exception: Serialization is not implemented
        .Where(sample => sample.ClickHouseType != "Nothing")
        .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

    [Test]
    [Parallelizable]
    [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
    public async Task ShouldExecuteParameterizedCompareWithTypeDetection(string exampleExpression, string clickHouseType, object value)
    {
        // https://github.com/ClickHouse/ClickHouse/issues/33928
        // TODO: remove
        if (connection.ServerVersion.StartsWith("22.1.") && clickHouseType == "IPv6")
            Assert.Ignore("IPv6 is broken in ClickHouse 22.1.2.2");

        if (clickHouseType.StartsWith("DateTime64") || clickHouseType == "Date" || clickHouseType == "Date32")
            Assert.Pass("Automatic type detection does not work for " + clickHouseType);
        if (clickHouseType.StartsWith("Enum"))
            clickHouseType = "String";

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
        command.AddParameter("var", value);

        var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
        Assert.AreEqual(result[0], result[1]);

        if (value is null || value is DBNull)
        {
            Assert.IsInstanceOf<DBNull>(result[2]);
        }
    }

    public void Dispose() => connection?.Dispose();
}
