using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ClickHouseType
    {
        public abstract ClickHouseTypeCode TypeCode { get; }

        public abstract Type FrameworkType { get; }

        public abstract string ToHttpParameter(object value);

        public abstract string ToInlineParameter(object value);

        public abstract override string ToString();
    }
}
