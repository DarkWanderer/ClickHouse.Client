using System;
using System.Globalization;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal32Type : DecimalType
    {
        public Decimal32Type()
        {
            Precision = 9;
        }

        public override string Name => "Decimal32";

        public override int Size => 4;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new Decimal32Type
            {
                Scale = int.Parse(node.SingleChild.Value),
            };
        }
        
        public override string ToInlineParameter(object value) => $"toDecimal32({((decimal)value).ToString(CultureInfo.InvariantCulture)},{Scale})";

        public override string ToString() => $"{Name}({Scale})";
    }
}
