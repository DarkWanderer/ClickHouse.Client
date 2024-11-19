using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy.Serializer;

internal interface IRowSerializer
{
    void Serialize(object[] row, ClickHouseType[] types, ExtendedBinaryWriter writer);
}
