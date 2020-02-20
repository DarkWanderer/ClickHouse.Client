using System;
using ClickHouse.Client.Utility;
using NodaTime;

namespace ClickHouse.Client.Types
{
    internal class DateTime64Type : DateTimeType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.DateTime64;

        public override Type FrameworkType => typeof(DateTime);

        public int Scale { get; set; }

        public override string ToString() => $"DateTime64({Scale}, {TimeZone.Id})";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var parameters = typeName.Substring(Name.Length).TrimRoundBrackets().Split(',');

            var scale = int.Parse(parameters[0]);
            var timeZone = DateTimeZone.Utc;
            if (parameters.Length > 1)
            {
                var timeZoneName = parameters[1].Trim().Trim('\'');
                timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;
            }

            return new DateTime64Type
            {
                TimeZone = timeZone,
                Scale = scale
            };
        }
    }
}
