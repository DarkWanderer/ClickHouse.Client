using System;
using System.Globalization;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal128Type : DecimalType
    {
        public Decimal128Type()
        {
            Precision = 38;
        }

        public override int Size => 16;

        public override string Name => "Decimal128";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new Decimal128Type
            {
                Scale = int.Parse(node.SingleChild.Value),
            };
        }
        
        public override string ToInlineParameter(object value) => $"toDecimal128({((decimal)value).ToString(CultureInfo.InvariantCulture)},{Scale})";

        public override string ToString() => $"{Name}({Scale})";
    }
}
