using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class SimpleAggregateFunctionType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public string AggregateFunction { get; set; }

        public override Type FrameworkType => UnderlyingType.FrameworkType;

        public override string Name => "SimpleAggregateFunction";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new SimpleAggregateFunctionType
            {
                AggregateFunction = node.ChildNodes[0].Value,
                UnderlyingType = parseClickHouseTypeFunc(node.ChildNodes[1]),
            };
        }

        public override object Read(ExtendedBinaryReader reader) => UnderlyingType.Read(reader);

        public override string ToString() => $"{Name}({AggregateFunction}, {UnderlyingType})";

        public override void Write(ExtendedBinaryWriter writer, object value) => UnderlyingType.Write(writer, value);
    }
}
