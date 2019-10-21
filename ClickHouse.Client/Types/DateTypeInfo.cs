namespace ClickHouse.Client.Types
{
    internal class DateTypeInfo : DateTimeTypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Date;

        public override string Name => "Date";

        public override string ToString() => $"Date({TimeZone.Id})";
    }
}
