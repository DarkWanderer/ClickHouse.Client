using System;

namespace ClickHouse.Client.Types
{
    internal class Float32Type : FloatType
    {
        public override Type FrameworkType => typeof(float);

        public override string ToString() => "Float32";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
