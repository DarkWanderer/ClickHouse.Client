using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public class MathUtilsTests
{
    [Test]
    [TestCase(10, 10)]
    [TestCase(2, 29)]
    public void ShouldRaiseIntegerToPowersCorrectly(int integer, int powersMax)
    {
        ulong value = 1;
        for (int i = 1; i <= powersMax; i++)
        {
            value *= (ulong)integer;
            Assert.AreEqual(value, MathUtils.ToPower(integer, i));
        }
    }
}
