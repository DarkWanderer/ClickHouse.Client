using System;
using System.Collections.Generic;
using System.Linq;
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

        public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
            //.Where(sample => !new [] {"Enum", "DateTime64(9)"}.Contains(sample.ClickHouseType)) //old clh doesn`t know about regular Enum and DateTime64
            .Where(sample => sample.ExampleValue != DBNull.Value) //null value should be handled by writing "is null" statement
            .Where(sample => !sample.ClickHouseType.StartsWith("Tuple")) // Bug in Tuple(Nullable(...))
            .Where(sample => sample.ClickHouseType != "UUID") // https://github.com/ClickHouse/ClickHouse/issues/7463
            .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

        [Test]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedSelectWhereWithTypeDetection(string exampleExpression, string clickHouseType, object value)
        {
            if (clickHouseType.StartsWith("DateTime64") || clickHouseType == "Date")
                Assert.Pass("Automatic type detection does not work for " + clickHouseType);
            if (clickHouseType.StartsWith("Enum"))
                clickHouseType = "String";

            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT res FROM (SELECT {exampleExpression} AS res) WHERE res = {{var:{clickHouseType}}}";
            command.AddParameter("var", value);

            var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
            Assert.AreEqual(value, result);
        }

        [Test]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedSelectWithExplicitType(string exampleExpression, string clickHouseType, object value)
        {
            if (clickHouseType.StartsWith("Enum"))
                clickHouseType = "String";
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {{var:{clickHouseType}}} as res";
            command.AddParameter("var", clickHouseType, value);

            var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow().Single();
            Assert.AreEqual(value, result);
        }

        [Test]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedSelectWhereWithExplicitType(string exampleExpression, string clickHouseType, object value)
        {
            if (clickHouseType.StartsWith("Enum"))
                clickHouseType = "String";
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {exampleExpression} AS expected, ({{var:{clickHouseType}}}) as actual";
            command.AddParameter("var", clickHouseType, value);

            var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
            Assert.AreEqual(result[0], result[1]);
        }
    }
}
