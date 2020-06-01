namespace ClickHouse.Client.Types
{
    internal class Enum16Type : EnumType
    {
        public override string Name => "Enum16";

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Enum16;

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);
    }
}
