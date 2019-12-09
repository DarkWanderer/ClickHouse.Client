namespace ClickHouse.Client.Types
{
    internal class DateTypeInfo : DateTimeTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Date;

        public override string Name => "Date";

        public override string ToString() => $"Date({TimeZone.Id})";
    }
}
