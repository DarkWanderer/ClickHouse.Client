using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ClickHouseType
    {
        public abstract ClickHouseTypeCode TypeCode { get; }

        public abstract Type FrameworkType { get; }

        public abstract override string ToString();
    }
}
