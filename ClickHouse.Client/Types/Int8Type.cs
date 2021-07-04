using System;

namespace ClickHouse.Client.Types
{
    internal class Int8Type : IntegerType
    {
        public override Type FrameworkType => typeof(sbyte);

        public override string ToString() => "Int8";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
