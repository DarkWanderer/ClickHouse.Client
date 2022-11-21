﻿using System;
using NodaTime;

namespace ClickHouse.Client.Types;

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
        return TimeZone.AtLeniently(LocalDateTime.FromDateTime(dateTime));
    }

    public DateTimeOffset ToDateTimeOffset(DateTime dateTime) => ToZonedDateTime(dateTime).ToDateTimeOffset();

    public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}({TimeZone.Id})";

    private DateTime ToDateTime(Instant instant)
    {
        // Special case for ETC/GMT timezone. TODO: support other aliases like Etc/Universal
        if (TimeZone == null)
            return instant.ToDateTimeUtc();
        var zonedDateTime = instant.InZone(TimeZone);
        if (zonedDateTime.Offset.Ticks == 0)
            return zonedDateTime.ToDateTimeUtc();
        else
            return zonedDateTime.ToDateTimeUnspecified();
    }
}
