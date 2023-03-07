using NUnit.Framework;

namespace ClickHouse.Client.Tests.Attributes;

/// <summary>
/// IgnoreInVersionAttribute tests
/// Designed to work across 'regression' runs under different versions
/// </summary>
public class IgnoreInVersionAttributeTests : AbstractConnectionTestFixture
{
    [Test]
    [IgnoreInVersion(21)]
    public void ShouldNotRunInVersion21() => Assert.That(!connection.ServerVersion.StartsWith("21"));

    [Test]
    [IgnoreInVersion(22)]
    public void ShouldNotRunInVersion22() => Assert.That(!connection.ServerVersion.StartsWith("22"));

    [Test]
    [IgnoreInVersion(21)]
    [IgnoreInVersion(22)]
    public void ShouldNotRunInVersion21or22()
    {
        Assert.That(!connection.ServerVersion.StartsWith("21"));
        Assert.That(!connection.ServerVersion.StartsWith("22"));
    }

    [Test]
    [IgnoreInVersion(22, 6)]
    public void ShouldNotRunInVersion22dot6() => Assert.That(!connection.ServerVersion.StartsWith("22.6"));
}
