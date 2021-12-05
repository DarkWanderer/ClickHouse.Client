using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;
using NodaTime;

namespace ClickHouse.Client.Types
{
    internal class DateTimeType : AbstractDateTimeType
    {
        public override string Name => "DateTime";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            var timeZoneName = node.ChildNodes.Count > 0 ? node.SingleChild.Value.Trim('\'') : string.Empty;
            var timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName) ?? DateTimeZone.Utc;

            return new DateTimeType { TimeZone = timeZone };
        }

        public override object Read(ExtendedBinaryReader reader) => FromUnixTimeSeconds(reader.ReadUInt32());

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var dto = value is DateTimeOffset offset ? offset : ToDateTimeOffset((DateTime)value);
            writer.Write((int)dto.ToUnixTimeSeconds());
        }
    }
}
