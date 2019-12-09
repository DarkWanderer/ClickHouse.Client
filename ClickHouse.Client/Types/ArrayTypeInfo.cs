using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class ArrayTypeInfo : ParameterizedTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Array;

        public ClickHouseType UnderlyingType { get; set; }

        public override Type EquivalentType => UnderlyingType.EquivalentType.MakeArrayType();

        public override string Name => "Array";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new ArrayTypeInfo
            {
                UnderlyingType = typeResolverFunc(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

        public override string ToString() => $"Array({UnderlyingType.ToString()})";
    }
}
