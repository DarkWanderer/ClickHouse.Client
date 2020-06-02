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


        [Test]
        public async Task ShouldExecuteSelectWithTupleParameter()
        {
            var sql = @"
                SELECT 1
                FROM (SELECT tuple(1, 'a', NULL) AS res)
                WHERE res.1 = tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 1)
                  AND res.2 = tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 2)
                  AND res.3 is NULL 
                  AND tupleElement({var:Tuple(Int32, String, Nullable(Int32))}, 3) is NULL";
            using var command = connection.CreateCommand();
            command.CommandText = sql;

            command.AddParameter("var", Tuple.Create<int, string, int?>(1, "a", null));

            var result = await command.ExecuteReaderAsync();
            result.GetEnsureSingleRow();
        }

        [Test]
        public async Task ShouldExecuteSelectWithUnderlyingTupleParameter()
        {
            var sql = @"
                SELECT 1
                FROM (SELECT tuple(123, tuple(5, 'a', 7)) AS res)
                WHERE res.1 = tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 1)
                  AND res.2.1 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 1)
                  AND res.2.2 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 2)
                  AND res.2.3 = tupleElement(tupleElement({var:Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))}, 2), 3)";
            using var command = connection.CreateCommand();
            command.CommandText = sql;

            command.AddParameter("var", Tuple.Create(123, Tuple.Create((byte)5, "a", 7)));

            var result = await command.ExecuteReaderAsync();
            result.GetEnsureSingleRow();
        }

        [Test]
        public async Task ShouldExecuteSelectWithIntArrayParameter()
        {
            var sql = @"
                SELECT 1
                FROM (SELECT array(1, 2, 3) AS res)
                WHERE hasAll(res, {var:Array(Int32)}) 
                  AND hasAll({var:Array(Int32)}, res)";
            using var command = connection.CreateCommand();
            command.CommandText = sql;

            command.AddParameter("var", new[] { 1, 2, 3 });

            var result = await command.ExecuteReaderAsync();
            result.GetEnsureSingleRow();
        }

        [Test]
        public async Task ShouldExecuteSelectWithStringArrayParameter()
        {
            var sql = @"
                SELECT 1
                FROM (SELECT array('x', '\'', '&') AS res)
                WHERE hasAll(res, {var:Array(String)}) 
                  AND hasAll({var:Array(String)}, res)";
            using var command = connection.CreateCommand();
            command.CommandText = sql;

            command.AddParameter("var", new[] { "x", "'", "&" });

            var result = await command.ExecuteReaderAsync();
            result.GetEnsureSingleRow();
        }
    }
}
