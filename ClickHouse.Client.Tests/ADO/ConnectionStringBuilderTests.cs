using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class ConnectionStringBuilderTests
{
    [Test]
    public void ShouldHaveReasonableDefaults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new ClickHouseConnectionStringBuilder().Port, Is.EqualTo(8123));
            Assert.That(new ClickHouseConnectionStringBuilder("Protocol=https").Port, Is.EqualTo(8443));
            Assert.That(new ClickHouseConnectionStringBuilder().Database, Is.EqualTo("default"));
            Assert.That(new ClickHouseConnectionStringBuilder().Username, Is.EqualTo("default"));
        });
    }
}
