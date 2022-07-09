using System;
using System.Globalization;
using System.Linq;
using ClickHouse.Client.Numerics;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Numerics
{
    [Parallelizable(ParallelScope.All)]
    [TestFixture]
    public class ClickHouseDecimalTests
    {
        static ClickHouseDecimalTests()
        {
            ClickHouseDecimal.MaxPrecision = 28;
        }

        public static readonly decimal[] Decimals = new decimal[]
        {
            -10000000000m,
            -9876543210m,
            -5478689523m,
            -45979752m,
            -1.234m,
            -0.7777m,
            -1.0m,
            -0.1m,
            -0.00000001m,
            0,
            0.000000001m,
            0.000003m,
            0.1m,
            0.19374596m,
            1.0m,
            2.0m,
            3.14159265359m,
            10,
            1000000,
            2000000,
            10000000000m,
        };

        public static readonly decimal[] DecimalsWithoutZero = Decimals.Where(d => d != 0).ToArray();

        public static readonly CultureInfo[] Cultures = new CultureInfo[]
        {
            CultureInfo.InvariantCulture,
            CultureInfo.GetCultureInfo("en-US"),
            CultureInfo.GetCultureInfo("zh-CN"),
            CultureInfo.GetCultureInfo("ru-RU"),
            CultureInfo.GetCultureInfo("ar-SA"),
        };

        [Test]
        [TestCase(1, 0, ExpectedResult = 0)]
        [TestCase(1, -1, ExpectedResult = -1)]
        [TestCase(1000, 0, ExpectedResult = 3)]
        [TestCase(1, 6, ExpectedResult = 6)]
        [TestCase(1000, 6, ExpectedResult = 9)]
        public int ShouldNormalize(long mantissa, int exponent) => new ClickHouseDecimal(mantissa, exponent).Exponent;

        [Test]
        [TestCase(1, ExpectedResult = 1)]
        [TestCase(1000, ExpectedResult = 1)]
        [TestCase(3900, ExpectedResult = 39)]
        [TestCase(1.234d, ExpectedResult = 1234)]
        public long ShouldConvert(decimal value) => (long)((ClickHouseDecimal)value).Mantissa;


        [Test, Combinatorial]
        public void ShouldAdd(
            [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
            [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
        {
            decimal expected = left + right;
            var actual = (ClickHouseDecimal)left + (ClickHouseDecimal)right;
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test, Combinatorial]
        public void ShouldFormat([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal value,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(Cultures))] CultureInfo culture)
        {
            var expected = value.ToString(culture);
            var actual = ((ClickHouseDecimal)value).ToString(culture);
            Assert.AreEqual(expected, actual);
        }

        [Test, Combinatorial]
        public void ShouldSubtract([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
        {
            decimal expected = left - right;
            var actual = (ClickHouseDecimal)left - (ClickHouseDecimal)right;
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test, Combinatorial]
        public void ShouldMultiply([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
        {
            decimal expected = left * right;
            var actual = (ClickHouseDecimal)left * (ClickHouseDecimal)right;
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test, Combinatorial]
        public void ShouldDivide([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithoutZero))] decimal right)
        {
            decimal expected = left / right;
            var actual = (ClickHouseDecimal)left / (ClickHouseDecimal)right;
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test, Combinatorial]
        public void ShouldDivideWithRemainder([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithoutZero))] decimal right)
        {
            decimal expected = left % right;
            var actual = (ClickHouseDecimal)left % (ClickHouseDecimal)right;
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test, Combinatorial]
        public void ShouldCompare([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
        {
            decimal expected = left.CompareTo(right);
            var actual = ((ClickHouseDecimal)left).CompareTo((ClickHouseDecimal)right);
            Assert.AreEqual(expected, (decimal)actual);
        }

        [Test]
        [TestCase(typeof(byte))]
        [TestCase(typeof(sbyte))]
        [TestCase(typeof(short))]
        [TestCase(typeof(ushort))]
        [TestCase(typeof(int))]
        [TestCase(typeof(uint))]
        [TestCase(typeof(long))]
        [TestCase(typeof(ulong))]
        [TestCase(typeof(float))]
        [TestCase(typeof(double))]
        [TestCase(typeof(decimal))]
        public void ShouldConvertToType(Type type)
        {
            ClickHouseDecimal source = 0m;
            var result = Convert.ChangeType(source, type);
            Assert.AreEqual(source.ToString(), result.ToString());
        }

        [Test, Ignore("Cannot get it to work yet")]
        [TestCase(typeof(byte))]
        [TestCase(typeof(sbyte))]
        [TestCase(typeof(short))]
        [TestCase(typeof(ushort))]
        [TestCase(typeof(int))]
        [TestCase(typeof(uint))]
        [TestCase(typeof(long))]
        [TestCase(typeof(ulong))]
        [TestCase(typeof(float))]
        [TestCase(typeof(double))]
        [TestCase(typeof(decimal))]
        public void ShouldConvertFromType(Type type)
        {
            var source = Convert.ChangeType(0, type);
            var result = Convert.ChangeType(0, typeof(ClickHouseDecimal));
            Assert.AreEqual(source.ToString(), result.ToString());
        }
    }
}
