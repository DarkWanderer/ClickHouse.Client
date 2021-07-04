using System;

namespace ClickHouse.Client.Types
{
    internal class Int16Type : IntegerType
    {
        public override Type FrameworkType => typeof(short);

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);

        public override string ToString() => "Int16";
    }
}
