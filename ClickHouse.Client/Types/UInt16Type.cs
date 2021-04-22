using System;

namespace ClickHouse.Client.Types
{
    internal class UInt16Type : IntegerType
    {
        public override Type FrameworkType => typeof(ushort);

        public override string ToString() => "UInt16";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
