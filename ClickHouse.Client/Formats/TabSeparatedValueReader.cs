using System;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class TabSeparatedValueReader : IStreamDataReader
    {
        public object ReadValue(ClickHouseType type) => throw new NotImplementedException();
    }

    internal class JsonCompactDataReader : IStreamDataReader
    {
        public object ReadValue(ClickHouseType type) => throw new NotImplementedException();
    }

    internal class JsonCompactDataWriter : IStreamDataWriter
    {
        public void WriteValue(object value, ClickHouseType type) => throw new NotImplementedException();
    }
}
