using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Misc;

public class ExtensionsTests
{
    [Test]
    public void ShouldDeconstruct2()
    {
        var (a1, a2) = new[] { 1, 2 };
        Assert.AreEqual(1, a1);
        Assert.AreEqual(2, a2);
    }

    [Test]
    public void ShouldDeconstruct3()
    {
        var (b1, b2, b3) = new[] { 1, 2, 3 };
        Assert.AreEqual(1, b1);
        Assert.AreEqual(2, b2);
        Assert.AreEqual(3, b3);
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
