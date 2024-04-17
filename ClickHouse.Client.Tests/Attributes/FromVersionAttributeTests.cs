using NUnit.Framework;

namespace ClickHouse.Client.Tests.Attributes;

/// <summary>
/// IgnoreInVersionAttribute tests
/// Designed to work across 'regression' runs under different versions
/// </summary>
public class FromVersionAttributeTests : AbstractConnectionTestFixture
{
    [Test]
    [FromVersion(23)]
    public void ShouldRunFromVersion23()
    {
        if (TestUtilities.ServerVersion != null)
            Assert.That(TestUtilities.ServerVersion.Major >= 23);
    }

    [Test]
    [FromVersion(23, 3)]
    public void ShouldNotRunInVersion22()
    {
        if (TestUtilities.ServerVersion != null)
        {
            Assert.That(TestUtilities.ServerVersion.Major >= 23);
            Assert.That(TestUtilities.ServerVersion.Minor >= 3);
        }
    }
}
