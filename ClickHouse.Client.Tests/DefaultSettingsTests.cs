using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public class DefaultSettingsTests
{
    [Test]
    public void DefaultSettingsShouldMatch()
    {
        var builder = new ClickHouseConnectionStringBuilder();
        Assert.Multiple(() =>
        {
            Assert.That(builder.UseCustomDecimals, Is.EqualTo(true));
            Assert.That(builder.Compression, Is.EqualTo(true));
            Assert.That(builder.UseServerTimezone, Is.EqualTo(true));
            Assert.That(builder.UseSession, Is.EqualTo(false));
        });
    }
}
