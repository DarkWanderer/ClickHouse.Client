using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal interface IStreamDataReader
    {
        object ReadValue(ClickHouseType type);
    }
}
