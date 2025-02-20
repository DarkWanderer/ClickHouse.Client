using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Constraints;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Tests.Attributes;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.BulkCopy;

public class BulkCopyWithDefaultsTests : AbstractConnectionTestFixture
{
    private static IEnumerable<TestCaseData> Get()
    {
        yield return new TestCaseData("UInt32 default 42", DBDefault.Value, 42, "UInt32_default");
        yield return new TestCaseData("UInt32 default 42", 10, 10, "UInt32_not_default");
        yield return new TestCaseData("Nullable(UInt32) default 42", DBDefault.Value, 42, "Nullable_UInt32_default");
        yield return new TestCaseData("Nullable(UInt32) default null", DBDefault.Value, DBNull.Value, "Nullable_UInt32_default_null");
        yield return new TestCaseData("Nullable(UInt32) default 42", 10, 10, "Nullable_UInt32_not_default");
        yield return new TestCaseData("Nullable(UInt32) default 42", DBNull.Value, DBNull.Value, "Nullable_UInt32_not_default_null");
        yield return new TestCaseData("String default 'foo'", DBDefault.Value, "foo", "String_default");
        yield return new TestCaseData("String default 'foo'", "bar", "bar", "String_not_default");
        yield return new TestCaseData("Date default toDate(now())", DBDefault.Value, DateTime.Today, "DateTime_default");
        yield return new TestCaseData("Date default toDate(now())", new DateTime(2003, 5, 2), new DateTime(2003, 5, 2), "DateTime_not_default");
    }

    [FromVersion(23, 7)]
    [TestCaseSource(typeof(BulkCopyWithDefaultsTests), nameof(Get))]
    public async Task ShouldExecuteSingleValueInsertViaBulkCopyWithDefaults(string clickhouseType, object insertValue, object expectedValue, string tableName)
    {
        var targetTable = "test." + SanitizeTableName($"bulk_single_default_{tableName}");

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE IF NOT EXISTS {targetTable} (`value` {clickhouseType}) ENGINE Memory");

        using var bulkCopyWithDefaults = new ClickHouseBulkCopy(connection, RowBinaryFormat.RowBinaryWithDefaults)
        {
            DestinationTableName = targetTable,
            MaxDegreeOfParallelism = 2,
            BatchSize = 100
        };

        await bulkCopyWithDefaults.InitAsync();
        await bulkCopyWithDefaults.WriteToServerAsync(Enumerable.Repeat(new[] { insertValue }, 1));

        Assert.That(bulkCopyWithDefaults.RowsWritten, Is.EqualTo(1));

        using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");

        Assert.That(await reader.ReadAsync(), Is.True, "Cannot read inserted data");
        Assert.That(reader.FieldCount, Is.EqualTo(1));

        var actualValue = reader.GetValue(0);
        Assert.That(actualValue, Is.EqualTo(expectedValue), "Original and actually inserted values differ");
    }
}
