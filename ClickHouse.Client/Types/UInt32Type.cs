using System;
using System.Globalization;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class UInt32Type : IntegerType
    {
        public override Type FrameworkType => typeof(uint);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt32();

        public override string ToString() => "UInt32";

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToUInt32(value, CultureInfo.InvariantCulture));
    }
}
