using System;
using System.Globalization;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class Int8Type : IntegerType
    {
        public override Type FrameworkType => typeof(sbyte);

        public override string ToString() => "Int8";

        public override object Read(ExtendedBinaryReader reader) => reader.ReadSByte();

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToSByte(value, CultureInfo.InvariantCulture));
    }
}
