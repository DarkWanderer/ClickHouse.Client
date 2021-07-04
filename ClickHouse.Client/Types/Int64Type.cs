using System;

namespace ClickHouse.Client.Types
{
    internal class Int64Type : IntegerType
    {
        public override Type FrameworkType => typeof(long);

        public override string ToString() => "Int64";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
