using System;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Misc;

public class ExtensionsTests
{
    [Test]
    public void ShouldDeconstruct2()
    {
        var (a1, a2) = new[] { 1, 2 };
        Assert.Multiple(() =>
        {
            Assert.That(a1, Is.EqualTo(1));
            Assert.That(a2, Is.EqualTo(2));
        });
    }

    [Test]
    public void ShouldDeconstruct3()
    {
        var (b1, b2, b3) = new[] { 1, 2, 3 };
        Assert.Multiple(() =>
        {
            Assert.That(b1, Is.EqualTo(1));
            Assert.That(b2, Is.EqualTo(2));
            Assert.That(b3, Is.EqualTo(3));
        });
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
