using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal64TypeInfo : DecimalTypeInfo
    {
        public Decimal64TypeInfo()
        {
            Precision = 18;
        }
        public override int Size => 8;

        public override string Name => "Decimal64";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new DecimalTypeInfo
            {
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }
        public override string ToString() => $"{Name}({Scale})";
    }
}
