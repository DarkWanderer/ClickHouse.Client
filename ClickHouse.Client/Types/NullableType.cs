using System;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class NullableType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Nullable;

        public ClickHouseType UnderlyingType { get; set; }

        public override Type FrameworkType
        {
            get
            {
                var underlyingFrameworkType = UnderlyingType.FrameworkType;
                return underlyingFrameworkType.IsValueType ? typeof(Nullable<>).MakeGenericType(underlyingFrameworkType) : underlyingFrameworkType;
            }
        }

        public override string Name => "Nullable";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new NullableType
            {
                UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
            };
        }

        public override string ToString() => $"{Name}({UnderlyingType.ToString()})";
    }
}
