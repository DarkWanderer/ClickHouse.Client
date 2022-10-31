using System.Threading.Tasks;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public class DateTimeTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldGetDateTimeOffsetFromNullable()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toNullable(toDateTime('2033-01-01 12:34:56', 'Europe/Moscow'))");
        Assert.IsTrue(reader.Read());
        var datetime = reader.GetDateTimeOffset(0);
        Assert.IsFalse(reader.Read());
        Assert.NotNull(datetime);
    }
}
