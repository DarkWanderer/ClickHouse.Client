using System;
using System.Linq;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class NestedType : TupleType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Nested;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new NestedType
            {
                UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
            };
        }

        public override Type FrameworkType => base.FrameworkType.MakeArrayType();
    }
}
