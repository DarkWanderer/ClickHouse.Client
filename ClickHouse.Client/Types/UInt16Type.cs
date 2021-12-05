using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class UInt16Type : IntegerType
    {
        public override Type FrameworkType => typeof(ushort);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt16();

        public override string ToString() => "UInt16";

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToUInt16(value));
    }
}
