using System;

namespace ClickHouse.Client.Types
{
    internal class DateType : DateTimeType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Date;

        public override string Name => "Date";

        public override string ToString() => $"Date({TimeZone.Id})";
    }
}
