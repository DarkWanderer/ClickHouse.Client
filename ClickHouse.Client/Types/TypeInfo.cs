using System;

namespace ClickHouse.Client.Types
{
    internal abstract class TypeInfo
    {
        public abstract ClickHouseDataType DataType { get; }

        public abstract Type EquivalentType { get; }

        public abstract override string ToString();
    }
}
