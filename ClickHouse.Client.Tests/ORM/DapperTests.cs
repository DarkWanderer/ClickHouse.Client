using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Utility;
using Dapper;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ORM;

public class DapperTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
        .Where(s => ShouldBeSupportedByDapper(s.ClickHouseType))
        .Where(s => s.ExampleValue != DBNull.Value)
        .Where(s => !s.ClickHouseType.StartsWith("Array")) // Dapper issue, see ShouldExecuteSelectWithParameters test
        .Select(sample => new TestCaseData($"SELECT {{value:{sample.ClickHouseType}}}", sample.ExampleValue));

    static DapperTests()
    {
        SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
        SqlMapper.AddTypeHandler(new ClickHouseDecimalHandler());
        SqlMapper.AddTypeHandler(new ITupleHandler());
    }

    // "The member value of type <xxxxxxxx> cannot be used as a parameter value"
    private static bool ShouldBeSupportedByDapper(string clickHouseType)
    {
        if (clickHouseType.Contains("Tuple"))
            return false;
        if (clickHouseType.Contains("Map"))
            return false;
        if (clickHouseType.Contains("Int128"))
            return false;
        if (clickHouseType.Contains("Int256"))
            return false;
        switch (clickHouseType)
        {
            case "UUID":
            case "Date":
            case "Date32":
            case "Nothing":
            case "IPv4":
            case "IPv6":
            case "Point":
            case "Ring":
                return false;
            default:
                return true;
        }
    }

    private class ITupleHandler : SqlMapper.TypeHandler<ITuple>
    {
        public override void SetValue(IDbDataParameter parameter, ITuple value) => parameter.Value = value;

        public override ITuple Parse(object value)
        {
            if (value is ITuple it)
                return it;
            throw new NotSupportedException();
        }
    }

    private class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
    {
        public override void SetValue(IDbDataParameter parameter, DateTimeOffset value) => parameter.Value = value.UtcDateTime;

        public override DateTimeOffset Parse(object value) => DateTimeOffset.Parse((string)value);
    }

    private class ClickHouseDecimalHandler : SqlMapper.TypeHandler<ClickHouseDecimal>
    {
        public override void SetValue(IDbDataParameter parameter, ClickHouseDecimal value) => parameter.Value = value.ToString(CultureInfo.InvariantCulture);

        public override ClickHouseDecimal Parse(object value) => value switch
        {
            ClickHouseDecimal chd => chd,
            IConvertible ic => Convert.ToDecimal(ic),
            _ => throw new ArgumentException(nameof(value))
        };
    }

    [Test]
    public async Task ShouldExecuteSimpleSelect()
    {
        string sql = "SELECT * FROM system.table_functions";

        var functions = (await connection.QueryAsync<string>(sql)).ToList();
        CollectionAssert.IsNotEmpty(functions);
        CollectionAssert.AllItemsAreNotNull(functions);
    }

    [Test]
    [Parallelizable]
    [TestCaseSource(typeof(DapperTests), nameof(SimpleSelectQueries))]
    public async Task ShouldExecuteSelectWithSingleParameterValue(string sql, object value)
    {
        var parameters = new Dictionary<string, object> { { "value", value } };
        var results = await connection.QueryAsync<string>(sql, parameters);
        Assert.AreEqual(Convert.ToString(value, CultureInfo.InvariantCulture), results.Single());
    }

    [Test]
    public async Task ShouldExecuteSelectWithArrayParameter()
    {
        var parameters = new Dictionary<string, object> { { "names", new[] { "mysql", "odbc" } } };
        string sql = "SELECT * FROM system.table_functions WHERE has({names:Array(String)}, name)";

        var functions = (await connection.QueryAsync<string>(sql, parameters)).ToList();
        CollectionAssert.IsNotEmpty(functions);
        CollectionAssert.AllItemsAreNotNull(functions);
    }

    [Test]
    public async Task ShouldExecuteSelectReturningArray()
    {
        string sql = "SELECT array(1,2,3)";
        var result = (await connection.QueryAsync<int[]>(sql)).Single();
        CollectionAssert.IsNotEmpty(result);
        CollectionAssert.AllItemsAreNotNull(result);
    }

    [Test]
    public async Task ShouldExecuteSelectReturningTuple()
    {
        string sql = "SELECT tuple(1,2,3)";
        var result = (await connection.QueryAsync<ITuple>(sql)).Single();
        Assert.IsInstanceOf<ITuple>(result);
        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, result.AsEnumerable());
    }
}
