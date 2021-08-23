using System;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class LowCardinalityType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public override string Name => "LowCardinality";

        public override Type FrameworkType => UnderlyingType.FrameworkType;

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => UnderlyingType.AcceptRead(reader);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => UnderlyingType.AcceptWrite(writer, value);

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new LowCardinalityType
            {
                UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
            };
        }

        public override string ToString() => $"{Name}({UnderlyingType})";
    }
}
