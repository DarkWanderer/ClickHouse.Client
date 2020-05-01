using System;
using System.Linq;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class NestedType : TupleType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Nested;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new NestedType
            {
                UnderlyingTypes = node.ChildNodes.Select(typeResolverFunc).ToArray(),
            };
        }

        public override Type FrameworkType => base.FrameworkType.MakeArrayType();
    }
}
