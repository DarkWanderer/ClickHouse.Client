using System;
using NodaTime;

namespace ClickHouse.Client.Types
{
    internal class DateTimeTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.DateTime;

        public override Type EquivalentType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public override string ToString() => $"DateTime({TimeZone.Id})";

        internal static DateTimeTypeInfo ParseTimeZone(string timeZoneName)
        {
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;
            return new DateTimeTypeInfo { TimeZone = timeZone };
        }
    }
}
