﻿using System;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;
using NodaTime;
using NodaTime.TimeZones;

namespace ClickHouse.Client.Types
{
    internal class DateTimeType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.DateTime;

        public override Type FrameworkType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public DateTimeOffset ToDateTimeOffset(DateTime dateTime)
        {
            switch (dateTime.Kind)
            {
                case DateTimeKind.Local:
                case DateTimeKind.Utc:
                    var instant = Instant.FromDateTimeUtc(dateTime.ToUniversalTime());
                    var offset = TimeZone.GetUtcOffset(instant);
                    return instant.WithOffset(offset).ToDateTimeOffset();
                case DateTimeKind.Unspecified:
                    if (TimeZone == null)
                    {
                        return dateTime;
                    }

                    var zonedDateTime = TimeZone.ResolveLocal(LocalDateTime.FromDateTime(dateTime), Resolvers.LenientResolver);
                    return zonedDateTime.ToDateTimeOffset();
            }
            throw new ArgumentOutOfRangeException("Unknown DateTime kind: " + dateTime.Kind.ToString());
        }

        public DateTime ToUtc(DateTime dateTime)
        {
            switch (dateTime.Kind)
            {
                case DateTimeKind.Local:
                case DateTimeKind.Utc:
                    return dateTime.ToUniversalTime();
                case DateTimeKind.Unspecified:
                    if (TimeZone == null)
                        return dateTime;

                    var zonedDateTime = TimeZone.ResolveLocal(LocalDateTime.FromDateTime(dateTime), Resolvers.LenientResolver);
                    return zonedDateTime.ToDateTimeUtc();
            }
            throw new ArgumentOutOfRangeException("Unknown DateTime kind: " + dateTime.Kind.ToString());
        }

        public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}({TimeZone.Id})";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            var timeZoneName = node.ChildNodes.Count > 0 ? node.SingleChild.Value.Trim('\'') : string.Empty;
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;

            return new DateTimeType { TimeZone = timeZone };
        }
    }
}
