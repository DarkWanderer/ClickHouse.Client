using System;
using System.Collections.Generic;
using System.Numerics;
using ClickHouse.Client.Numerics;

namespace ClickHouse.Client.Tests.Numerics;

public class ClickHouseInt128Tests
{
    public static BigInteger MaxValue = BigInteger.Parse("170141183460469231731687303715884105727");
    public static BigInteger MinValue = BigInteger.Parse("-170141183460469231731687303715884105728");

    public static IEnumerable<BigInteger> Fibonacci
    {
        get
        {
            // Base cases
            yield return 0;
            yield return 1;
            yield return int.MaxValue;
            yield return uint.MaxValue;
            yield return long.MaxValue;
            yield return ulong.MaxValue;

            // Fibonacci sequence for very large numbers
            bool setLeft = false;
            BigInteger right = 1;
            BigInteger left = 0;
            while (true)
            {
                var sum = left + right;
                if (sum > MaxValue)
                    yield break;
                yield return sum;
                if (setLeft)
                    left = sum;
                else
                    right = sum;
                setLeft = !setLeft;
            }
        }
    }

    [Test]
    [TestCaseSource(nameof(Fibonacci))]
    public void ShouldRoundtripConvert(BigInteger input)
    {
        var int128 = new ClickHouseInt128(input);
        var roundtrip = (BigInteger)int128;
        Assert.That(roundtrip, Is.EqualTo(input));
    }

    [Test]
    [TestCaseSource(nameof(Fibonacci))]
    public void ShouldCompareEquality(BigInteger input)
    {
        var value1 = new ClickHouseInt128(input);
        var value2 = new ClickHouseInt128(input + 1);
        var value3 = new ClickHouseInt128(input);
        Assert.That(value1, Is.Not.EqualTo(value2));
        Assert.That(value1, Is.EqualTo(value3));
    }

    [Test]
    [TestCaseSource(nameof(Fibonacci))]
    public void ShouldIncrement(BigInteger input)
    {
        var value = new ClickHouseInt128(input);
        value++;
        Assert.That((BigInteger)value, Is.EqualTo(input + 1));
    }

    [Test]
    public void ShouldHaveCorrectExtremeValues()
    {
        Assert.That((BigInteger)ClickHouseInt128.MaxValue, Is.EqualTo(MaxValue));
        Assert.That((BigInteger)ClickHouseInt128.MinValue, Is.EqualTo(MinValue));
    }
}
