using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BulkCopyTests
    {
        private readonly ClickHouseConnection connection = TestUtilities.GetTestClickHouseConnection();

        [SetUp]
        public Task FixtureSetup() => connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS temp");

        public static IEnumerable<TestCaseData> GetInsertSingleValueTestCases()
        {
            foreach (var sample in TestUtilities.GetDataTypeSamples())
            {
                if (new[] { "Enum8", "Nothing", "Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))" }.Contains(sample.ClickHouseType))
                    continue;
                yield return new TestCaseData(sample.ClickHouseType, sample.ExampleValue);
            }
            yield return new TestCaseData("String", "1\t2\n3");
            yield return new TestCaseData("DateTime('Asia/Ashkhabad')", new DateTime(2020, 2, 20, 20, 20, 20, DateTimeKind.Utc));
            //yield return new TestCaseData("DateTime('Asia/Almaty')", new DateTime(2020, 2, 20, 20, 20, 20, DateTimeKind.Local));
            //yield return new TestCaseData("DateTime('Asia/Anadyr')", new DateTime(2020, 2, 20, 20, 20, 20, DateTimeKind.Unspecified));
            // yield return new TestCaseData("Nested(A UInt8, B String)", new[] { Tuple.Create(1, "AAA"), Tuple.Create(2, "BBB") });
        }

        [Test]
        [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
        public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
        {
            var targetTable = SanitizeTableName($"temp.b_{clickHouseType}");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {clickHouseType}) ENGINE Log");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { insertedValue }, 1));

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
            Assert.IsTrue(reader.Read(), "Cannot read inserted data");
            reader.AssertHasFieldCount(1);
            var data = reader.GetValue(0);
            Assert.AreEqual(insertedValue, data);
        }

        [Test]
        public async Task ShouldExecuteInsertWithLessColumns()
        {
            var targetTable = $"temp.multiple_columns";

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value1 Nullable(UInt8), value2 Nullable(Float32), value3 Nullable(Int8)) ENGINE Log");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5 }, 5), new[] { "value2" }, CancellationToken.None);

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        }

        [Test]
        [Ignore("Dropped support for this use case for now")]
        public async Task ShouldExecuteInsertWithColumnsExpression()
        {
            var targetTable = $"temp.multiple_columns";

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value1 Nullable(UInt8), value2 Nullable(Float32), value3 Nullable(Int8)) ENGINE Log");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5 }, 5), new[] { "COLUMNS('e2')" }, CancellationToken.None);

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        }

        [Test]
        public async Task ShouldExecuteInsertWithBacktickedColumns()
        {
            var targetTable = $"temp.backticked_columns";

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`field.id` Nullable(UInt8), `@value` Nullable(UInt8)) ENGINE Log");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5, 5 }, 5), new[] { "`field.id`, `@value`" });

            using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {targetTable}");
        }

        private string SanitizeTableName(string input)
        {
            var builder = new StringBuilder();
            foreach (var c in input)
            {
                if (char.IsLetterOrDigit(c))
                    builder.Append(c);
                else if (c == '_' || c == '.')
                    builder.Append(c);
            }
            return builder.ToString();
        }
    }
}
