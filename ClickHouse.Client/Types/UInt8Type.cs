using System;

namespace ClickHouse.Client.Types
{
    internal class UInt8Type : IntegerType
    {
        public override Type FrameworkType => typeof(byte);

        public override string ToString() => "UInt8";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
