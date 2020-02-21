using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class LowCardinalityType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.LowCardinality;

        public override string Name => "LowCardinality";

        public override Type FrameworkType => UnderlyingType.FrameworkType;

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new LowCardinalityType
            {
                UnderlyingType = typeResolverFunc(typeName.Substring(Name.Length).TrimRoundBrackets()),
            };
        }

        public override string ToString() => $"{Name}({UnderlyingType.ToString()})";
    }
}
