using System;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy.Serializer;

internal class RowBinarySerializer : IRowSerializer
{
    public void Serialize(Span<object> row, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < row.Length; col++)
        {
            types[col].Write(writer, row[col]);
        }
    }
}
