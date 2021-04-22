using System;

namespace ClickHouse.Client.Types
{
    internal class UInt64Type : IntegerType
    {
        public override Type FrameworkType => typeof(ulong);

        public override string ToString() => "UInt64";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
