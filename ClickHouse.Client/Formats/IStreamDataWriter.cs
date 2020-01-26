using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal interface IStreamDataWriter
    {
        void WriteValue(object value, ClickHouseType type);
    }
}
