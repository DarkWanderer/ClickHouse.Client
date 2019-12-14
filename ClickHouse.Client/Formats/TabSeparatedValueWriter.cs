using System;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class TabSeparatedValueWriter : IStreamDataWriter
    {
        public void WriteValue(object value, ClickHouseType type) => throw new NotImplementedException();
    }
}
