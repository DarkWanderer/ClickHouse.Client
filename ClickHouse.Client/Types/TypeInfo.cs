using System;

namespace ClickHouse.Client.Types
{
    internal abstract class TypeInfo
    {
        public abstract ClickHouseDataType DataType { get; }

        public abstract Type EquivalentType { get; }

        public abstract override string ToString();
    }

    internal class FixedStringTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.FixedString;

        public int Length { get; set; }

        public override Type EquivalentType => typeof(string);

        public override string ToString() => $"FixedString{Length}";
    }

    internal class ArrayTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Array;

        public TypeInfo UnderlyingType { get; set; }

        public override Type EquivalentType => UnderlyingType.EquivalentType.MakeArrayType();

        public override string ToString() => $"Array({UnderlyingType.ToString()})";
    }

    internal class NullableTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Nullable;

        public TypeInfo UnderlyingType { get; set; }

        public override Type EquivalentType => typeof(Nullable<>).MakeGenericType(UnderlyingType.EquivalentType);

        public override string ToString() => $"Nullable({UnderlyingType.ToString()})";
    }

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
