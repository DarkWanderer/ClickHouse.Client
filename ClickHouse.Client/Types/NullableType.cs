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

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new NullableType
            {
                UnderlyingType = typeResolverFunc(node.SingleChild),
            };
        }

        public override string ToHttpParameter(object value) => value is null || value == DBNull.Value ?
            "null" :
            $"{UnderlyingType.ToHttpParameter(value)}";
        
        public override string ToInlineParameter(object value) => value is null || value == DBNull.Value ?
            "null" :
            $"{UnderlyingType.ToHttpParameter(value)}";

        public override string ToString() => $"{Name}({UnderlyingType.ToString()})";
    }
}
