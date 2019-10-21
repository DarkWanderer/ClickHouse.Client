using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ClickHouseTypeInfo
    {
        public abstract ClickHouseDataType DataType { get; }

        public abstract Type EquivalentType { get; }

        public abstract override string ToString();
    }


}
