using System;
using System.Collections.Generic;
using System.Data;
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
    public class SqlParameterizedSelectTests : IDisposable
    {
        private readonly ClickHouseConnection connection;

        public SqlParameterizedSelectTests(bool useCompression)
        {
            connection = TestUtilities.GetTestClickHouseConnection(useCompression);
            connection.Open();
        }

        public static IEnumerable<TestCaseData> TypedQueryParameters => TestUtilities.GetDataTypeSamples()
            // DB::Exception: There are no UInt128 literals in SQL
            .Where(sample => !sample.ClickHouseType.Contains("UUID") || TestUtilities.SupportedFeatures.HasFlag(FeatureFlags.SupportsUUIDParameters))
            // DB::Exception: Serialization is not implemented
            .Where(sample => sample.ClickHouseType != "Nothing")
            .Select(sample => new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue));

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedCompareWithTypeDetection(string exampleExpression, string clickHouseType, object value)
        {
            if (!connection.SupportedFeatures.HasFlag(FeatureFlags.SupportsDateTime64) && clickHouseType.StartsWith("DateTime"))
                Assert.Pass("ClickHouse 19.x DateTime comparison is bugged");
            if (clickHouseType.StartsWith("DateTime64") || clickHouseType == "Date")
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
            else
            {
                Assert.AreEqual(1, result[2], $"Equality check in ClickHouse failed: {result[0]} {result[1]}");
            }
        }

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedSelectWithExplicitType(string _, string clickHouseType, object value)
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
        [Parallelizable]
        [TestCaseSource(typeof(SqlParameterizedSelectTests), nameof(TypedQueryParameters))]
        public async Task ShouldExecuteParameterizedCompareWithExplicitType(string exampleExpression, string clickHouseType, object value)
        {
            if (!connection.SupportedFeatures.HasFlag(FeatureFlags.SupportsDateTime64) && clickHouseType.StartsWith("DateTime"))
                Assert.Pass("ClickHouse 19.x DateTime comparison is bugged");
            if (clickHouseType.StartsWith("Enum"))
                clickHouseType = "String";
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {exampleExpression} as expected, {{var:{clickHouseType}}} as actual, expected = actual as equals";
            command.AddParameter("var", clickHouseType, value);

            var result = (await command.ExecuteReaderAsync()).GetEnsureSingleRow();
            Assert.AreEqual(result[0], result[1]);

            if (value is null || value is DBNull)
            {
                Assert.IsInstanceOf<DBNull>(result[2]);
            }
            else
            {
                Assert.AreEqual(1, result[2], $"Equality check in ClickHouse failed: {result[0]} {result[1]}");
            }
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

        public void Dispose() => connection?.Dispose();
    }
}
