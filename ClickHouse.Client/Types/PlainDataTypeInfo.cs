using System;

namespace ClickHouse.Client.Types
{
    internal class PlainDataTypeInfo<T> : TypeInfo
    {
        private readonly ClickHouseDataType dataType;

        public PlainDataTypeInfo(ClickHouseDataType dataType)
        {
            this.dataType = dataType;
            EquivalentType = typeof(T);
        }

        public override Type EquivalentType { get; }

        public override ClickHouseDataType DataType => dataType;

        public override string ToString() => DataType.ToString();
    }
}
