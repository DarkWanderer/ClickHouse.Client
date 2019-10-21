using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class FixedStringTypeInfo : ParameterizedTypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.FixedString;

        public int Length { get; set; }

        public override Type EquivalentType => typeof(string);

        public override string Name => "FixedString";
        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new FixedStringTypeInfo
            {
                Length = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

        public override string ToString() => $"FixedString{Length}";
    }
}
