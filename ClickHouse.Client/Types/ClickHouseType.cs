using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ClickHouseType
    {
        public abstract ClickHouseTypeCode DataType { get; }

        public abstract Type EquivalentType { get; }

        public abstract override string ToString();
    }
}
