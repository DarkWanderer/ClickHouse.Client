using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;
using NodaTime;

namespace ClickHouse.Client.Types;

internal class DateTimeType : AbstractDateTimeType
{
    public override string Name => "DateTime";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        DateTimeZone timeZone = null;
        if (node.ChildNodes.Count > 0)
        {
            var timeZoneName = node.ChildNodes[0].Value.Trim('\'');
            timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName);
        }
        timeZone ??= DateTimeZoneProviders.Tzdb.GetZoneOrNull(settings.timezone);

        return new DateTimeType { TimeZone = timeZone };
    }

    public override object Read(ExtendedBinaryReader reader) => FromUnixTimeSeconds(reader.ReadUInt32());

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var dto = value is DateTimeOffset offset ? offset : ToDateTimeOffset((DateTime)value);
        writer.Write((int)dto.ToUnixTimeSeconds());
    }
}
