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
            yield return new TestCaseData("DateTime('Asia/Ashkhabad')", new DateTime(2020, 2, 20, 20, 20, 20, DateTimeKind.Unspecified));
        }

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
        public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
        {
            var targetTable = "test." + SanitizeTableName($"bulk_single_{clickHouseType}");

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
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value1 Nullable(UInt8), value2 Nullable(Float32), value3 Nullable(Int8)) ENGINE TinyLog");

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
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`field.id` Nullable(UInt8), `@value` Nullable(UInt8)) ENGINE TinyLog");

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
        [TestCase("with!exclamation")]
        public async Task ShouldExecuteBulkInsertWithComplexColumnName(string columnName)
        {
            var targetTable = "test." + SanitizeTableName($"bulk_complex_{columnName}");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`{columnName.Replace("`", "\\`")}` Int32) ENGINE TinyLog");

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
        [RequiredFeature(Feature.InlineQuery)]
        public async Task ShouldInsertIntoTableWithLotsOfColumns()
        {
            var tableName = "test.bulk_long_columns";
            var columnCount = 3900;

            //Generating create tbl statement with a lot of columns 
            var query = $"CREATE TABLE IF NOT EXISTS {tableName}(\n";
            var columns = Enumerable.Range(1, columnCount)
                .Select(x => $" some_loooooooooooooonnnnnnnnnnnngggggggg_column_name_{x} Int32");
            query += string.Join(",\n", columns);
            query += ")\n ENGINE = MergeTree()\n ORDER BY (some_loooooooooooooonnnnnnnnnnnngggggggg_column_name_1)";

            //Create tbl in db
            await connection.ExecuteStatementAsync(query);

            var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };

            var rowToInsert = new[] { Enumerable.Range(1, columnCount).Select(x => (object)x).ToArray() };
            await bulkCopy.WriteToServerAsync(rowToInsert);
        }

        [Test]
        public async Task ShouldThrowSpecialExceptionOnSerializationFailure()
        {
            var targetTable = "test." + SanitizeTableName($"bulk_exception_uint8");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value UInt8) ENGINE Memory");

            var rows = Enumerable.Range(250, 10).Select(n => new object[] { n }).ToArray();

            var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
            try
            {
                await bulkCopy.WriteToServerAsync(rows);
                Assert.Fail("Bulk copy did not throw exception on failed serialization");
            }
            catch (ClickHouseBulkCopySerializationException ex)
            {
                CollectionAssert.AreEqual(new object[] { 256 }, ex.Row);
                Assert.AreEqual(0, ex.Index);
                Assert.IsInstanceOf<OverflowException>(ex.InnerException);
            }
        }

        [Test]
        public async Task ShouldExecuteBulkInsertIntoSimpleAggregatedFunctionColumn()
        {
            var targetTable = "test." + SanitizeTableName($"bulk_simple_aggregated_function");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value SimpleAggregateFunction(anyLast,Nullable(Float64))) ENGINE TinyLog");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
                MaxDegreeOfParallelism = 2,
                BatchSize = 100
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { (object)1 }, 1), CancellationToken.None);

            Assert.AreEqual(1, bulkCopy.RowsWritten);
            // Verify we can read back
            Assert.AreEqual(1, await connection.ExecuteScalarAsync($"SELECT value FROM {targetTable}"));
        }


        [Test]
        public async Task ShouldNotLoseRowsOnMuptipleBatches()
        {
            var targetTable = "test.bulk_multiple_batches"; ;

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Int32) ENGINE TinyLog");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
                MaxDegreeOfParallelism = 2,
                BatchSize = 10
            };

            const int Count = 1000;
            var data = Enumerable.Repeat(new object[] { 1 }, Count);

            await bulkCopy.WriteToServerAsync(data, CancellationToken.None);

            Assert.AreEqual(Count, bulkCopy.RowsWritten);
            Assert.AreEqual(Count, await connection.ExecuteScalarAsync($"SELECT count() FROM {targetTable}"));
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
