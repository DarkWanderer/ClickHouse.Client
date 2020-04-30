using System;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal64Type : DecimalType
    {
        public Decimal64Type()
        {
            Precision = 18;
        }

        public override int Size => 8;

        public override string Name => "Decimal64";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new Decimal64Type
            {
                Scale = int.Parse(node.SingleChild.Value),
            };
        }

        public override string ToString() => $"{Name}({Scale})";
    }
}
