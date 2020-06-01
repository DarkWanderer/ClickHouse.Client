namespace ClickHouse.Client.Types
{
    internal class Enum8Type : EnumType
    {
        public override string Name => "Enum8";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);
    }
}
