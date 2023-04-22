using System;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using Dapper.Contrib.Extensions;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ORM;

[TestFixture]
public class DapperContribTests : AbstractConnectionTestFixture
{
    // TODO: Non-UTC timezones
    // TODO: DateTimeTimeOffset
    private readonly static TestRecord referenceRecord = new(1, "value", new DateTime(2023, 4, 15, 1, 2, 3, DateTimeKind.Utc));

    [Table("test.dapper_contrib")]
    public record class TestRecord(int Id, string Value, DateTime Timestamp);

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_contrib");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_contrib (Id Int32, Value String, Timestamp DateTime('UTC')) ENGINE TinyLog");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_contrib VALUES (1, 'value', toDateTime('2023/04/15 01:02:03', 'UTC'))");
    }

    [Test]
    public async Task ShouldGetAll() => CollectionAssert.Contains(await connection.GetAllAsync<TestRecord>(), referenceRecord);

    [Test]
    public async Task ShouldGet() => Assert.AreEqual(referenceRecord, await connection.GetAsync<TestRecord>(1));

    [Test]
    [Ignore("Dapper.Contrib does not properly support ClickHouse yet")]
    public async Task ShouldInsert() => await connection.InsertAsync(referenceRecord);
}
