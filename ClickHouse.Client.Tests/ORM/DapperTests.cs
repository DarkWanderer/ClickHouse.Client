using System.Threading.Tasks;
using NUnit.Framework;
using Dapper;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System;

namespace ClickHouse.Client.Tests.ORM
{
    public class DapperTests : AbstractConnectionTestFixture
    {
        public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
            .Where(s => ShouldBeSupportedByDapper(s.ClickHouseType))
            .Where(s => s.ExampleValue != DBNull.Value)
            .Where(s => !s.ClickHouseType.StartsWith("Array")) // Dapper issue, see ShouldExecuteSelectWithParameters test
            .Select(sample => new TestCaseData($"SELECT {{value:{sample.ClickHouseType}}}", sample.ExampleValue));

        // "The member value of type <xxxxxxxx> cannot be used as a parameter value"
        private static bool ShouldBeSupportedByDapper(string clickHouseType)
        {
            if (clickHouseType.StartsWith("Tuple") || clickHouseType.StartsWith("IPv"))
                return false;
            if (clickHouseType == "UUID" || clickHouseType == "Date" || clickHouseType == "Nothing")
                return false;

            return true;
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
        [Ignore("Requires Dapper support, see https://github.com/StackExchange/Dapper/pull/1462")]
        public async Task ShouldExecuteSelectWithArrayParameter()
        {
            var parameters = new Dictionary<string, object> { { "names", new[] { "mysql", "odbc" } } };
            string sql = "SELECT * FROM system.table_functions WHERE has({names:Array(String)}, name)";

            var functions = (await connection.QueryAsync<string>(sql, parameters)).ToList();
            CollectionAssert.IsNotEmpty(functions);
            CollectionAssert.AllItemsAreNotNull(functions);
        }

        [Test]
        [Ignore("Requires Dapper support, see https://github.com/StackExchange/Dapper/pull/1467")]
        public async Task ShouldExecuteSelectReturningArray()
        {
            string sql = "SELECT array(1,2,3)";
            var result = (await connection.QueryAsync<int[]>(sql)).Single();
            CollectionAssert.IsNotEmpty(result);
            CollectionAssert.AllItemsAreNotNull(result);
        }
    }
}
