using System;
using ClickHouse.Client.Utility;
using NodaTime;

namespace ClickHouse.Client.Types
{

    internal class DateTimeType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.DateTime64;

        public override Type FrameworkType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public override string Name => "DateTime";

        public override string ToString() => $"DateTime({TimeZone.Id})";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var timeZoneName = typeName.Substring(Name.Length).TrimRoundBrackets();
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;

            return new DateTimeType
            {
                TimeZone = timeZone
            };
        }
    }

    internal class DateTime64Type : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.DateTime;

        public override Type FrameworkType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public override string Name => "DateTime64";

        public override string ToString() => $"DateTime({TimeZone.Id})";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var timeZoneName = typeName.Substring(Name.Length).TrimRoundBrackets();
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;

            return new DateTime64Type
            {
                TimeZone = timeZone
            };
        }
    }
}
