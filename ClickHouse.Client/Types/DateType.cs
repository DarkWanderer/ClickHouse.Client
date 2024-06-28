using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

internal class DateType : AbstractDateTimeType
{
#if NET6_0_OR_GREATER
    public override Type FrameworkType => typeof(DateOnly);
#endif

    public override string Name { get; }

    public override string ToString() => "Date";

    public override object Read(ExtendedBinaryReader reader)
    {
#if NET6_0_OR_GREATER
        return DateOnlyEpochStart.AddDays(reader.ReadUInt16());
#else
        return DateTimeEpochStart.AddDays(reader.ReadUInt16());
#endif
    }

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings) => throw new NotImplementedException();

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
#if NET6_0_OR_GREATER
        if (value is DateOnly @do)
        {
            var delta = @do.DayNumber - DateOnlyEpochStart.DayNumber;
            writer.Write((ushort)delta);
            return;
        }
#endif
        var sinceEpoch = ((DateTime)value).Date - DateTimeEpochStart;
        writer.Write(Convert.ToUInt16(sinceEpoch.TotalDays));
    }
}
