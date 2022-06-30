using System;
using NodaTime;
using NodaTime.TimeZones;

namespace ClickHouse.Client.Types
{
    internal abstract class AbstractDateTimeType : ParameterizedType
    {
        // DateTime.UnixEpoch is not available on .NET 4.8
        public static readonly DateTime DateTimeEpochStart = DateTimeOffset.FromUnixTimeSeconds(0).UtcDateTime;

        public override Type FrameworkType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public DateTime FromUnixTimeTicks(long ticks) => ToDateTime(Instant.FromUnixTimeTicks(ticks));

        public DateTime FromUnixTimeSeconds(long seconds) => ToDateTime(Instant.FromUnixTimeSeconds(seconds));

        public ZonedDateTime ToZonedDateTime(DateTime dateTime)
        {
            var timeZone = TimeZone ?? DateTimeZone.Utc;
            return TimeZone.ResolveLocal(LocalDateTime.FromDateTime(dateTime), Resolvers.LenientResolver);
        }

        public DateTimeOffset ToDateTimeOffset(DateTime dateTime) => ToZonedDateTime(dateTime).ToDateTimeOffset();

        public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}({TimeZone.Id})";

        private DateTime ToDateTime(Instant instant) => TimeZone != null ? instant.InZone(TimeZone).ToDateTimeUnspecified() : instant.ToDateTimeUtc();
    }
}
