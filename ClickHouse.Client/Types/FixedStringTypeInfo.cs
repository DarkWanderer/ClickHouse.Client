using System;

namespace ClickHouse.Client.Types
{
    internal class FixedStringTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.FixedString;

        public int Length { get; set; }

        public override Type EquivalentType => typeof(string);

        public override string ToString() => $"FixedString{Length}";
    }
}
