using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Tests.Misc;

public class ExtensionsTests
{
    [Test]
    public void ShouldDeconstruct2()
    {
        var (a1, a2) = new[] { 1, 2 };
        ClassicAssert.AreEqual(1, a1);
        ClassicAssert.AreEqual(2, a2);
    }

    [Test]
    public void ShouldDeconstruct3()
    {
        var (b1, b2, b3) = new[] { 1, 2, 3 };
        ClassicAssert.AreEqual(1, b1);
        ClassicAssert.AreEqual(2, b2);
        ClassicAssert.AreEqual(3, b3);
    }

    [Test]
    public void ShouldThrowOnWrongCount()
    {
        Assert.Throws<ArgumentException>(() => { var (a1, a2) = new[] { 1 }; });
        Assert.Throws<ArgumentException>(() => { var (a1, a2) = new[] { 1, 2, 3 }; });
        Assert.Throws<ArgumentException>(() => { var (a1, a2, a3) = new[] { 1 }; });
        Assert.Throws<ArgumentException>(() => { var (a1, a2, a3) = new[] { 1, 2, 3, 4 }; });
    }
}
