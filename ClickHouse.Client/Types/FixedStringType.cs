using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class FixedStringType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.FixedString;

        public int Length { get; set; }

        public override Type FrameworkType => typeof(string);

        public override string Name => "FixedString";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new FixedStringType
            {
                Length = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

        public override string ToString() => $"FixedString{Length}";
    }
}
