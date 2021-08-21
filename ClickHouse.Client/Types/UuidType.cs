using System;

namespace ClickHouse.Client.Types
{
    internal class UuidType : ClickHouseType
    {
        public override Type FrameworkType => typeof(Guid);

        public override string ToString() => "UUID";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
