using System;
using ClickHouse.Client.Utility;
using NodaTime;

namespace ClickHouse.Client.Types
{

    internal class DateTimeTypeInfo : ParameterizedTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.DateTime;

        public override Type EquivalentType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public override string Name => "DateTime";

        public override string ToString() => $"DateTime({TimeZone.Id})";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var timeZoneName = typeName.Substring(Name.Length).TrimRoundBrackets();
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;

            return new DateTimeTypeInfo
            {
                TimeZone = timeZone
            };
        }
    }
}
