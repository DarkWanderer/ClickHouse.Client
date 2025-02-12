using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Tests.ADO;

public class ConnectionStringBuilderTests
{
    [Test]
    public void ShouldHaveReasonableDefaults()
    {
        ClassicAssert.AreEqual(8123, new ClickHouseConnectionStringBuilder().Port);
        ClassicAssert.AreEqual(8443, new ClickHouseConnectionStringBuilder("Protocol=https").Port);
        ClassicAssert.AreEqual("default", new ClickHouseConnectionStringBuilder().Database);
        ClassicAssert.AreEqual("default", new ClickHouseConnectionStringBuilder().Username);
    }
}
