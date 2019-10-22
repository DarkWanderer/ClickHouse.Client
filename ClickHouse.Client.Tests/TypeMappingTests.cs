using System;
using ClickHouse.Client.Types;
using NUnit.Framework;
namespace ClickHouse.Client.Tests
{
    public class TypeMappingTests
    {
        [Test]
        [TestCase("Int8", ExpectedResult = typeof(sbyte))]
        [TestCase("Int16", ExpectedResult = typeof(short))]
        [TestCase("Int32", ExpectedResult = typeof(int))]
        [TestCase("Int64", ExpectedResult = typeof(long))]

        [TestCase("UInt8", ExpectedResult = typeof(byte))]
        [TestCase("UInt16", ExpectedResult = typeof(ushort))]
        [TestCase("UInt32", ExpectedResult = typeof(uint))]
        [TestCase("UInt64", ExpectedResult = typeof(ulong))]

        [TestCase("Float32", ExpectedResult = typeof(float))]
        [TestCase("Float64", ExpectedResult = typeof(double))]

        [TestCase("Decimal(18,3)", ExpectedResult = typeof(decimal))]
        [TestCase("Decimal32(3)", ExpectedResult = typeof(decimal))]
        [TestCase("Decimal64(3)", ExpectedResult = typeof(decimal))]
        [TestCase("Decimal128(3)", ExpectedResult = typeof(decimal))]

        [TestCase("FixedString(5)", ExpectedResult = typeof(string))]

        [TestCase("Date", ExpectedResult = typeof(DateTime))]
        [TestCase("DateTime", ExpectedResult = typeof(DateTime))]
        [TestCase("DateTime('Etc/UTC')", ExpectedResult = typeof(DateTime))]

        [TestCase("Nullable(UInt32)", ExpectedResult = typeof(uint?))]
        [TestCase("Array(Array(String))", ExpectedResult = typeof(string[][]))]
        [TestCase("Array(Nullable(UInt32))", ExpectedResult = typeof(uint?[]))]
        public Type ShouldConvertFromClickHouseType(string clickHouseType) => TypeConverter.ParseClickHouseType(clickHouseType).EquivalentType;

        [Test]
        [TestCase(typeof(sbyte), ExpectedResult = "Int8")]
        [TestCase(typeof(short), ExpectedResult = "Int16")]
        [TestCase(typeof(int), ExpectedResult = "Int32")]
        [TestCase(typeof(long), ExpectedResult = "Int64")]

        [TestCase(typeof(byte), ExpectedResult = "UInt8")]
        [TestCase(typeof(ushort), ExpectedResult = "UInt16")]
        [TestCase(typeof(uint), ExpectedResult = "UInt32")]
        [TestCase(typeof(ulong), ExpectedResult = "UInt64")]

        [TestCase(typeof(float), ExpectedResult = "Float32")]
        [TestCase(typeof(double), ExpectedResult = "Float64")]
        [TestCase(typeof(decimal), ExpectedResult = "Decimal128(0)")]

        [TestCase(typeof(string), ExpectedResult = "String")]

        [TestCase(typeof(DateTime), ExpectedResult = "DateTime")]

        [TestCase(typeof(uint?), ExpectedResult = "Nullable(UInt32)")]
        [TestCase(typeof(string[][]), ExpectedResult = "Array(Array(String))")]
        [TestCase(typeof(uint?[]), ExpectedResult = "Array(Nullable(UInt32))")]
        [TestCase(typeof(Tuple<int,byte,float?,string[]>), ExpectedResult="Tuple(Int32,UInt8,Nullable(Float32),Array(String))")]
        public string ShouldConvertToClickHouseType(Type type) => TypeConverter.ToClickHouseType(type).ToString();
    }
}
