using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class DateType : AbstractDateTimeType
    {
        public override string Name { get; }

        public override string ToString() => "Date";

        public override object Read(ExtendedBinaryReader reader) => DateTimeEpochStart.AddDays(reader.ReadUInt16());

        public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc) => throw new NotImplementedException();

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var sinceEpoch = ((DateTime)value).Date - DateTimeEpochStart;
            writer.Write(Convert.ToUInt16(sinceEpoch.TotalDays));
        }
    }
}
