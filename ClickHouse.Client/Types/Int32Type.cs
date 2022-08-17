using System;
using System.Globalization;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class Int32Type : IntegerType
    {
        public override Type FrameworkType => typeof(int);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadInt32();

        public override string ToString() => "Int32";

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToInt32(value, CultureInfo.InvariantCulture));
    }
}
