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
    public class SqlInlineParametersSelectTest
    {
        private class OldClickHouseVersionConnection : ClickHouseConnection
        {
            public OldClickHouseVersionConnection(string connectionString) : base(connectionString) { }

            public override Task<bool> SupportsHttpParameters() => Task.FromResult(false);
        }

        private readonly ClickHouseConnection connection;

        public SqlInlineParametersSelectTest(bool useCompression)
        {
            var connectionStringBuilder = TestUtilities.GetConnectionStringBuilder();
            connectionStringBuilder.Compression = useCompression;
            connection = new OldClickHouseVersionConnection(connectionStringBuilder.ToString());
        }

        public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
            .Where(sample => sample.ClickHouseType != "UUID") // https://github.com/ClickHouse/ClickHouse/issues/7463
            .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

        [Test]
        public async Task EnsureCompatibilityModeIsUsed() => Assert.IsFalse(await connection.SupportsHttpParameters());

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
    }
}
