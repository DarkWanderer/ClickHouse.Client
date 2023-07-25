using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

internal class Date32Type : DateType
{
    public override string Name { get; }

    public override string ToString() => "Date32";

    public override object Read(ExtendedBinaryReader reader)
    {
        var days = reader.ReadInt32();
#if NET6_0_OR_GREATER
        return DateOnlyEpochStart.AddDays(days);
#else
        return DateTimeEpochStart.AddDays(days);
#endif
    }

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings) => throw new NotImplementedException();

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
#if NET6_0_OR_GREATER
        if (value is DateOnly @do)
        {
            var delta = @do.DayNumber - DateOnlyEpochStart.DayNumber;
            writer.Write((int)delta);
            return;
        }
#endif
        var sinceEpoch = ((DateTime)value).Date - DateTimeEpochStart;
        writer.Write(Convert.ToInt32(sinceEpoch.TotalDays));
    }
}
