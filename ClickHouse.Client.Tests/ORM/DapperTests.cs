using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ORM
{
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
        }

        // "The member value of type <xxxxxxxx> cannot be used as a parameter value"
        private static bool ShouldBeSupportedByDapper(string clickHouseType)
        {
            if (clickHouseType.Contains("Tuple"))
                return false;
            if (clickHouseType.Contains("Map"))
                return false;
            switch (clickHouseType)
            {
                case "UUID":
                case "Date":
                case "Date32":
                case "Nothing":
                case "IPv4":
                case "IPv6":
                    return false;
                default:
                    return true;
            }
        }

        private class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
        {
            public override void SetValue(IDbDataParameter parameter, DateTimeOffset value) => parameter.Value = value;

            public override DateTimeOffset Parse(object value)
                => DateTimeOffset.Parse((string)value);
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
            Assert.AreEqual(string.Format(CultureInfo.InvariantCulture, "{0}", value), results.Single());
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
    }
}
