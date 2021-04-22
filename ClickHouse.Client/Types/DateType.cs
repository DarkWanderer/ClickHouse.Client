using System;

namespace ClickHouse.Client.Types
{
    internal class DateType : DateTimeType
    {
        public override Type FrameworkType => typeof(DateTime);

        public override string ToString() => "Date";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
