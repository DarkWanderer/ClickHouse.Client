using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class NullableType : ParameterizedType
    {
        public ClickHouseType UnderlyingType { get; set; }

        public override Type FrameworkType
        {
            get
            {
                var underlyingFrameworkType = UnderlyingType.FrameworkType;
                return underlyingFrameworkType.IsValueType ? typeof(Nullable<>).MakeGenericType(underlyingFrameworkType) : underlyingFrameworkType;
            }
        }

        public override string Name => "Nullable";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
        {
            return new NullableType
            {
                UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
            };
        }

        public override object Read(ExtendedBinaryReader reader) => reader.ReadByte() > 0 ? DBNull.Value : UnderlyingType.Read(reader);

        public override string ToString() => $"{Name}({UnderlyingType})";

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            if (value == null || value is DBNull)
            {
                writer.Write((byte)1);
            }
            else
            {
                writer.Write((byte)0);
                UnderlyingType.Write(writer, value);
            }
        }
    }
}
