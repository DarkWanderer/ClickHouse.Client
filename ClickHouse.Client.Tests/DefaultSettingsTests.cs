using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Tests;

public class DefaultSettingsTests
{
    [Test]
    public void DefaultSettingsShouldMatch()
    {
        var builder = new ClickHouseConnectionStringBuilder();
        ClassicAssert.AreEqual(true, builder.UseCustomDecimals);
        ClassicAssert.AreEqual(true, builder.Compression);
        ClassicAssert.AreEqual(true, builder.UseServerTimezone);
        ClassicAssert.AreEqual(false, builder.UseSession);
    }
}
