using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class Date32Type : AbstractDateTimeType
    {
        public override string Name { get; }

        public override string ToString() => "Date32";

        public override object Read(ExtendedBinaryReader reader)
        {
            var days = reader.ReadInt32();
            return DateTimeEpochStart.AddDays(days);
        }

        public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc) => throw new NotImplementedException();

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var sinceEpoch = ((DateTime)value).Date - DateTimeEpochStart;
            writer.Write(Convert.ToInt32(sinceEpoch.TotalDays));
        }
    }
}
