using System;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class ArrayType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public override Type FrameworkType => UnderlyingType.FrameworkType.MakeArrayType();

        public override string Name => "Array";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new ArrayType
            {
                UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
            };
        }

        public Array MakeArray(int length) => Array.CreateInstance(UnderlyingType.FrameworkType, length);

        public override string ToString() => $"{Name}({UnderlyingType})";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
