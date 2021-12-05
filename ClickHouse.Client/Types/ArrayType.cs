using System;
using System.Collections;
using ClickHouse.Client.Formats;
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

        public override object Read(ExtendedBinaryReader reader)
        {
            var length = reader.Read7BitEncodedInt();
            var data = MakeArray(length);
            for (var i = 0; i < length; i++)
            {
                data.SetValue(ClearDBNull(UnderlyingType.Read(reader)), i);
            }
            return data;
        }

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var collection = (IList)value;
            writer.Write7BitEncodedInt(collection.Count);
            for (var i = 0; i < collection.Count; i++)
            {
                UnderlyingType.Write(writer, collection[i]);
            }
        }
    }
}
