using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class ConnectionStringBuilderTests
{
    [Test]
    public void ShouldHaveReasonableDefaults()
    {
        Assert.AreEqual(8123, new ClickHouseConnectionStringBuilder().Port);
        Assert.AreEqual(8443, new ClickHouseConnectionStringBuilder("Protocol=https").Port);
        Assert.AreEqual("default", new ClickHouseConnectionStringBuilder().Database);
        Assert.AreEqual("default", new ClickHouseConnectionStringBuilder().Username);
    }
}
