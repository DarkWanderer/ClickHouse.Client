using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class Float64Type : FloatType
    {
        public override Type FrameworkType => typeof(double);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadDouble();

        public override string ToString() => "Float64";

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToDouble(value));
    }
}
