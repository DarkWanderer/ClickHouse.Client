using System;

namespace ClickHouse.Client.Types
{
    internal class Int32Type : IntegerType
    {
        public override Type FrameworkType => typeof(int);

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);

        public override string ToString() => "Int32";
    }
}
