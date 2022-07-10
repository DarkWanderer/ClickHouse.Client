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
            -1000000000000m,
            -5478689523m,
            -459m,
            -1.234m,
            -0.7777m,
            -0.00000000001m,
            0,
            0.000000000001m,
            0.000003m,
            0.1m,
            0.19374596m,
            1.0m,
            1.000m,
            2.0m,
            3.14159265359m,
            10,
            1000000,
            1000000000000m,
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
        [TestCase(0.001, ExpectedResult = 3)]
        [TestCase(0.01, ExpectedResult = 2)]
        [TestCase(0.1, ExpectedResult = 1)]
        [TestCase(1, ExpectedResult = 0)]
        [TestCase(10, ExpectedResult = 0)]
        public int ShouldNormalizeScale(decimal @decimal) => new ClickHouseDecimal(@decimal).Scale;

        [Test]
        [TestCase(0.001, ExpectedResult = 1)]
        [TestCase(0.01, ExpectedResult = 1)]
        [TestCase(0.1, ExpectedResult = 1)]
        [TestCase(1, ExpectedResult = 1)]
        [TestCase(10, ExpectedResult = 10)]
        public long ShouldNormalizeMantissa(decimal value) => (long)((ClickHouseDecimal)value).Mantissa;

        [Test]
        [TestCase(12.345, 1, ExpectedResult = 12)]
        [TestCase(12.345, 2, ExpectedResult = 12)]
        [TestCase(12.345, 3, ExpectedResult = 12.3)]
        [TestCase(12.345, 4, ExpectedResult = 12.34)]
        [TestCase(12.345, 5, ExpectedResult = 12.345)]
        public decimal ShouldTruncate(decimal value, int precision) => (decimal)new ClickHouseDecimal(value).Truncate(precision);

        [Test]
        public void ShouldRoundtripConversion( [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal value)
        {
            var result = new ClickHouseDecimal(value);
            Assert.AreEqual(value, (decimal)result);
        }
            
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
        public void ShouldParse([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal expected,
                                    [ValueSource(typeof(ClickHouseDecimalTests), nameof(Cultures))] CultureInfo culture)
        {
            var actual = (decimal)ClickHouseDecimal.Parse(expected.ToString(culture), culture);
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
            int expected = left.CompareTo(right);
            int actual = ((ClickHouseDecimal)left).CompareTo((ClickHouseDecimal)right);
            Assert.AreEqual(expected, actual);
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
            ClickHouseDecimal source = 5m;
            var result = Convert.ChangeType(source, type);
            Assert.AreEqual(source.ToString(), result.ToString());
        }

        [Test]
        [Ignore("Not implemented")]
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
