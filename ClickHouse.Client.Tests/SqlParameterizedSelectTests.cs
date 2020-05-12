using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(true)]
    [TestFixture(false)]
    public class SqlParameterizedSelectTests
    {
        private readonly ClickHouseConnection connection;

        public SqlParameterizedSelectTests(bool useCompression)
        {
            connection = TestUtilities.GetTestClickHouseConnection(useCompression);
        }

        public static IEnumerable<TestCaseData> ParametersQueries => TestUtilities.GetDataTypeSamples()
            //.Where(sample => !new [] {"Enum", "DateTime64(9)"}.Contains(sample.ClickHouseType)) //old clh doesn`t know about regular Enum and DateTime64
            .Where(sample => sample.ExampleValue != DBNull.Value) //null value should be handled by writing "is null" statement
            .Where(sample => !sample.ClickHouseType.StartsWith("Array") && !sample.ClickHouseType.StartsWith("Tuple")) // complex types should be handled differently
            .Where(sample => sample.ClickHouseType != "Date") //DateTime with 00:00:00 can`t be Date type, cause it`ll broke DateTime logic. Specify type to fix that
            .Where(sample => sample.ClickHouseType != "DateTime64(9)") //Default DateTime CLH analog is DateTime. If you want to use DateTime64 specify type as parameter
            .Where(sample => sample.ClickHouseType != "UUID") // https://github.com/ClickHouse/ClickHouse/issues/7463
            .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

        [Test]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(ParametersQueries))]
        public async Task ShouldExecuteSelectWithParameters(string exampleExpression, string type, object value)
        {
            if (type.StartsWith("Enum"))
                type = "String";
            var sql = $"SELECT 1 FROM (SELECT {exampleExpression} AS res) WHERE res = {{var:{type}}}";
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.AddParameter("var", value);

            var result = await command.ExecuteReaderAsync();
            result.GetEnsureSingleRow();
        }
    }
}
