using System;

namespace ClickHouse.Client.Types
{
    internal class PlainDataType<T> : ClickHouseType
    {
        private readonly ClickHouseTypeCode dataType;

        public PlainDataType(ClickHouseTypeCode dataType)
        {
            this.dataType = dataType;
            FrameworkType = typeof(T);
        }

        public override Type FrameworkType { get; }

        public override ClickHouseTypeCode TypeCode => dataType;

        public override string ToString() => TypeCode.ToString();
    }
}
