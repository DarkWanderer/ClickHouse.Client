using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Copy.Serializer;
using ClickHouse.Client.Tests.Attributes;
using ClickHouse.Client.Utility;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace ClickHouse.Client.Tests.BulkCopy;

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
    [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
    public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
    {
        var targetTable = "test." + SanitizeTableName($"bulk_single_{clickHouseType}");

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {clickHouseType}) ENGINE Memory");

        var batchSentInvocationCount = 0L;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 100
        };

        bulkCopy.BatchSent += (sender, e) => Interlocked.Add(ref batchSentInvocationCount, e.RowsWritten);

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { insertedValue }, 1));

        Assert.That(batchSentInvocationCount, Is.EqualTo(1));
        Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        ClassicAssert.IsTrue(reader.Read(), "Cannot read inserted data");
        reader.AssertHasFieldCount(1);
        var data = reader.GetValue(0);
        Assert.That(data, Is.EqualTo(insertedValue).Using<JsonObject>(new JsonNodeEqualityComparer()), "Original and actually inserted values differ");
    }


#if NET6_0_OR_GREATER
    [Test]
    [RequiredFeature(Feature.Date32)]
    public async Task ShouldInsertDateOnly()
    {
        var targetTable = "test.bulk_dateonly";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Date32) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 100
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { new DateOnly(1999, 12, 31) }, 1));

        Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        ClassicAssert.IsTrue(reader.Read(), "Cannot read inserted data");
        reader.AssertHasFieldCount(1);
        var data = reader.GetValue(0);
        Assert.That(data, Is.EqualTo(new DateTime(1999, 12, 31)), "Original and actually inserted values differ");
    }
#endif

    [Test]
    [Explicit("Infinite loop test")]
    public async Task ShouldExecuteMultipleBulkInsertions()
    {
        var sw = new Stopwatch();
        var duration = TimeSpan.FromMinutes(5);

        var targetTable = "test." + SanitizeTableName($"bulk_load_test");

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (int Int32, str String, dt DateTime) ENGINE Null");

        var cb = TestUtilities.GetConnectionStringBuilder();
        cb.UseSession = false;

        var conn = new ClickHouseConnection(cb.ToString());
        sw.Start();
        var i = 0;
        try
        {
            while (sw.Elapsed < duration)
            {
                using var bulkCopy = new ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = targetTable,
                    MaxDegreeOfParallelism = 8,
                    BatchSize = 100
                };

                await bulkCopy.InitAsync();
                await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 0, "a", DateTime.Now }, 1000));
                i++;
            }
        }
        catch (Exception e)
        {
            throw new Exception($"Iteration: {i}", e);
        }
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
            ColumnNames = new[] { "value2" }
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5 }, 5), CancellationToken.None);

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
            ColumnNames = new[] { "`field.id`, `@value`" }
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 5, 5 }, 5));

        using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {targetTable}");
    }

    [Test]
    public async Task ShouldDetectColumnsAutomaticallyOnInit()
    {
        var targetTable = $"test.auto_detect_columns";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (field1 UInt8, field2 Int8, field3 String) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 1, 2, "3" }, 5));

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
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`{columnName.Replace("`", "\\`")}` Int32) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 100
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { (object)1 }, 1), CancellationToken.None);

        Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));
    }

    [Test]
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
        await bulkCopy.InitAsync();
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
        await bulkCopy.InitAsync();
        try
        {
            await bulkCopy.WriteToServerAsync(rows);
            Assert.Fail("Bulk copy did not throw exception on failed serialization");
        }
        catch (ClickHouseBulkCopySerializationException ex)
        {
            Assert.That(ex.Row, Is.EqualTo(new object[] { 256 }).AsCollection);
            ClassicAssert.IsInstanceOf<OverflowException>(ex.InnerException);
        }
    }

    [Test]
    public async Task ShouldExecuteBulkInsertIntoSimpleAggregatedFunctionColumn()
    {
        var targetTable = "test." + SanitizeTableName($"bulk_simple_aggregated_function");

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value SimpleAggregateFunction(anyLast,Nullable(Float64))) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 100
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { (object)1 }, 1), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));
            // Verify we can read back
            Assert.That(await connection.ExecuteScalarAsync($"SELECT value FROM {targetTable}"), Is.EqualTo(1));
        });
    }


    [Test]
    public async Task ShouldNotLoseRowsOnMultipleBatches()
    {
        var targetTable = "test.bulk_multiple_batches"; ;

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Int32) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 10
        };

        const int Count = 1000;
        var data = Enumerable.Repeat(new object[] { 1 }, Count);

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(data, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(bulkCopy.RowsWritten, Is.EqualTo(Count));
            Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {targetTable}"), Is.EqualTo(Count));
        });
    }

    [Test]
    public async Task ShouldExecuteWithDBNullArrays()
    {
        var targetTable = $"test.bulk_dbnull_array";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (stringValue Array(String), intValue Array(Int32)) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new List<object[]>
        {
            new object[] { DBNull.Value, new[] { 1, 2, 3 } },
            new object[] { new [] { "sample1", "sample2" }, DBNull.Value },
        }, CancellationToken.None);

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
    }

#if NET48 || NET5_0_OR_GREATER
    [Test]
    public async Task ShouldInsertNestedTable()
    {
        var targetTable = "test.bulk_nested";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (`_id` UUID, `Comments` Nested(Id Nullable(String), Comment Nullable(String))) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };

        await bulkCopy.InitAsync();

        await bulkCopy.WriteToServerAsync(new List<object[]>() { new object[] { Guid.NewGuid(), new ITuple[] { ("1", "Comment1"), ("2", "Comment2"), ("3", "Comment3") } } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        Assert.Multiple(async () =>
        {
            Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));
            Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {targetTable}"), Is.EqualTo(1));
        });
    }
#endif

    [Test]
    public async Task ShouldInsertDoubleNestedTable()
    {
        var targetTable = "test.bulk_double_nested";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (Id Int64, Threads Nested(Id Int64, Comments Nested(Id Int64, Text String))) ENGINE Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };

        await bulkCopy.InitAsync();

        var comments = new[]
        {
            Tuple.Create(1, "Comment 1"),
            Tuple.Create(2, "Comment 2"),
            Tuple.Create(3, "Comment 3"),
        };
        var threads = new[]
        {
            Tuple.Create(1, comments),
            Tuple.Create(2, comments),
            Tuple.Create(3, comments),
        };

        await bulkCopy.WriteToServerAsync([[1, threads]]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
        Assert.Multiple(async () =>
        {
            Assert.That(bulkCopy.RowsWritten, Is.EqualTo(1));
            Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {targetTable}"), Is.EqualTo(1));
        });
    }

    [Test]
    [TestCase(0.01)]
    [TestCase(0.25)]
    [TestCase(0.50)]
    [TestCase(0.75)]
    [TestCase(0.99)]
    public async Task ShouldThrowExceptionOnInnerException(double fraction)
    {
        const int setSize = 3000000;
        int dbNullIndex = (int)(setSize * fraction);

        var targetTable = "test." + SanitizeTableName($"bulk_million_inserts");


        var data = Enumerable.Repeat(new object[] { 1 }, setSize).ToArray();
        data[dbNullIndex][0] = DBNull.Value;

        //await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Int16) ENGINE Null");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 1000
        };

        await bulkCopy.InitAsync();

        Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () => await bulkCopy.WriteToServerAsync(data, CancellationToken.None));
    }

    [Test]
    public async Task ShouldNotAffectSharedArrayPool()
    {
        var targetTable = "test." + SanitizeTableName($"array_pool");

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (int Int32, str String, dt DateTime) ENGINE Null");

        const int poolSize = 8;
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            BatchSize = poolSize
        };

        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new object[] { 0, "a", DateTime.Now }, 100));

        var rentedArray = ArrayPool<object>.Shared.Rent(poolSize);
        Assert.DoesNotThrow(() => { rentedArray[0] = 1; });
        ArrayPool<object>.Shared.Return(rentedArray);
    }
}

