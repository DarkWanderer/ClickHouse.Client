using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BulkCopyTests : AbstractConnectionTestFixture
    {
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
        }

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
        public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
        {
            var targetTable = SanitizeTableName($"test.bulk_{clickHouseType}");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {clickHouseType}) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
                MaxDegreeOfParallelism = 2,
                BatchSize = 100
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { insertedValue }, 1));

            Assert.AreEqual(1, bulkCopy.RowsWritten);

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
            Assert.IsTrue(reader.Read(), "Cannot read inserted data");
            reader.AssertHasFieldCount(1);
            var data = reader.GetValue(0);
            Assert.AreEqual(insertedValue, data, "Original and actually inserted values differ");
        }

        [Test]
        public async Task ShouldExecuteInsertWithLessColumns()
        {
            var targetTable = $"test.multiple_columns";

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value1 Nullable(UInt8), value2 Nullable(Float32), value3 Nullable(Int8)) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5 }, 5), new[] { "value2" }, CancellationToken.None);

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        }

        [Test]
        public async Task ShouldExecuteInsertWithBacktickedColumns()
        {
            var targetTable = $"test.backticked_columns";

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`field.id` Nullable(UInt8), `@value` Nullable(UInt8)) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5, 5 }, 5), new[] { "`field.id`, `@value`" });

            using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {targetTable}");
        }

        [Test]
        [TestCase("with.dot")]
        [TestCase("with'quote")]
        [TestCase("double\"quote")]
        [TestCase("with space")]
        [TestCase("with`backtick")]
        [TestCase("with:colon")]
        [TestCase("with,comma")]
        [TestCase("with^caret")]
        [TestCase("with&ampersand")]
        [TestCase("with(round)brackets")]
        [TestCase("with*star")]
        [TestCase("with?question")]
        [TestCase("with?exclamation")]
        public async Task ShouldExecuteBulkInsertWithComplexColumnName(string columnName)
        {
            var targetTable = SanitizeTableName($"test.bulk_complex_{columnName}");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`{columnName.Replace("`", "\\`")}` Int32) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
                MaxDegreeOfParallelism = 2,
                BatchSize = 100
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { (object)1 }, 1), CancellationToken.None);

            Assert.AreEqual(1, bulkCopy.RowsWritten);
        }

        [Test]
        public async Task ShouldInsertIntoTableWithLotsOfColumns()
        {
            if (!await connection.SupportsInlineQuery())
                Assert.Inconclusive("This test is only valid for versions which support 'inline' POST body query");

            var tblName = "test.bulk_long_columns";
            var columnCount = 3900;

            //Generating create tbl statement with a lot of columns 
            var query = $"CREATE TABLE IF NOT EXISTS {tblName}(\n";
            var columns = Enumerable.Range(1, columnCount)
                .Select(x => $" some_loooooooooooooonnnnnnnnnnnngggggggg_column_name_{x} Int32");
            query += string.Join(",\n", columns);
            query += ")\n ENGINE = MergeTree()\n ORDER BY (some_loooooooooooooonnnnnnnnnnnngggggggg_column_name_1)";

            //Create tbl in db
            await connection.ExecuteStatementAsync(query);

            var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tblName };

            var rowToInsert = new[] { Enumerable.Range(1, columnCount).Select(x => (object)x).ToArray() };
            await bulkCopy.WriteToServerAsync(rowToInsert);
        }

        private string SanitizeTableName(string input)
        {
            var builder = new StringBuilder();
            foreach (var c in input)
            {
                if (char.IsLetterOrDigit(c) || c == '_')
                    builder.Append(c);
            }
            return builder.ToString();
        }
    }
}
