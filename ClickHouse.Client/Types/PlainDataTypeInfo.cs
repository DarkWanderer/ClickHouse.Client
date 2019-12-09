using System;

namespace ClickHouse.Client.Types
{
    internal class PlainDataTypeInfo<T> : ClickHouseType
    {
        private readonly ClickHouseTypeCode dataType;

        public PlainDataTypeInfo(ClickHouseTypeCode dataType)
        {
            this.dataType = dataType;
            EquivalentType = typeof(T);
        }

        public override Type EquivalentType { get; }

        public override ClickHouseTypeCode DataType => dataType;

        public override string ToString() => DataType.ToString();
    }
}
