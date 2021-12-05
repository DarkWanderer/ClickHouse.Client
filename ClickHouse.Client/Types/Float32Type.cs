using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class Float32Type : FloatType
    {
        public override Type FrameworkType => typeof(float);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadSingle();

        public override string ToString() => "Float32";

        public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToSingle(value));
    }
}
