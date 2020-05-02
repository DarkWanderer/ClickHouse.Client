using System;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class LowCardinalityType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.LowCardinality;

        public override string Name => "LowCardinality";

        public override Type FrameworkType => UnderlyingType.FrameworkType;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new LowCardinalityType
            {
                UnderlyingType = typeResolverFunc(node.SingleChild),
            };
        }
        
        public override string ToStringParameter(object value) => $"'{((string)value).Escape()}'";

        public override string ToString() => $"{Name}({UnderlyingType.ToString()})";
    }
}
