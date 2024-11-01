using ClickHouse.Client.Constraints;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy.Serializer;

// https://clickhouse.com/docs/en/interfaces/formats#rowbinarywithdefaults
internal class RowBinaryWithDefaultsSerializer : IRowSerializer
{
    public void Serialize(object[] row, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < row.Length; col++)
        {
            if (row[col] is DBDefault)
            {
                writer.Write(1);
            }
            else
            {
                writer.Write(0);
                types[col].Write(writer, row[col]);
            }
        }
    }
}
