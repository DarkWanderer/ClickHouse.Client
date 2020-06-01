using System;
using System.Net;

namespace ClickHouse.Client.Types
{
    internal class Int8Type : ClickHouseType
    {
        public override Type FrameworkType => typeof(sbyte);

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Int8;

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }

    internal class IPv4Type : ClickHouseType
    {
        public override Type FrameworkType => typeof(IPAddress);

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.IPv4;

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);
    }

    internal class IPv6Type : ClickHouseType
    {
        public override Type FrameworkType => typeof(IPAddress);

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.IPv6;

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);
    }
}
