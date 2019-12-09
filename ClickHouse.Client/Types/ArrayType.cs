using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class ArrayType : ParameterizedType
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Array;

        public ClickHouseType UnderlyingType { get; set; }

        public override Type EquivalentType => UnderlyingType.EquivalentType.MakeArrayType();

        public override string Name => "Array";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new ArrayType
            {
                UnderlyingType = typeResolverFunc(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

        public override string ToString() => $"Array({UnderlyingType.ToString()})";
    }
}
