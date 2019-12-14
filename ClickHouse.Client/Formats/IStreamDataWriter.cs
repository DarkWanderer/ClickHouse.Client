using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    interface IStreamDataWriter
    {
        void WriteValue(object value, ClickHouseType type);
    }
}
