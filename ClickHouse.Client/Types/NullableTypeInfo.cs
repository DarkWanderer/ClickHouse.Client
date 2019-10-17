using System;

namespace ClickHouse.Client.Types
{
    internal class NullableTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Nullable;

        public TypeInfo UnderlyingType { get; set; }

        public override Type EquivalentType => typeof(Nullable<>).MakeGenericType(UnderlyingType.EquivalentType);

        public override string ToString() => $"Nullable({UnderlyingType.ToString()})";
    }
}
