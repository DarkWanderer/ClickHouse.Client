using System;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Client.Types;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class TypeMappingTests
    {
        [Test]
        public void ShouldMapBasicTypesRoundtrip()
        {
            foreach (DataType initialType in Enum.GetValues(typeof(DataType)))
            {
                var type = TypeConverter.FromClickHouseSimpleType(initialType);
                var returnedType = TypeConverter.ToClickHouseType(type);

                if (initialType != DataType.Date && initialType != DataType.FixedString)
                    Assert.AreEqual(initialType.ToString(), returnedType);
            }
        }

        [Test]
        [TestCase("Nullable(UInt32)", ExpectedResult = typeof(uint?))]
        [TestCase("Array(Array(String))", ExpectedResult = typeof(string[][]))]
        [TestCase("Array(Nullable(UInt32))", ExpectedResult = typeof(uint?[]))]
        public Type ShouldParseComplexTypes(string clickHouseType) => TypeConverter.FromClickHouseType(clickHouseType);
    }
}
