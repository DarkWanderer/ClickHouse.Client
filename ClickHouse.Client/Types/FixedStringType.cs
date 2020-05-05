using System;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class FixedStringType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.FixedString;

        public int Length { get; set; }

        public override Type FrameworkType => typeof(string);

        public override string Name => "FixedString";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new FixedStringType
            {
                Length = int.Parse(node.SingleChild.Value),
            };
        }

        public override string ToHttpParameter(object value) => (string)value;

        public override string ToInlineParameter(object value) => ((string)value).Escape();

        public override string ToString() => $"FixedString{Length}";
    }
}
