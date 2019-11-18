using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal32TypeInfo : DecimalTypeInfo
    {
        public Decimal32TypeInfo()
        {
            Precision = 9;
        }

        public override string Name => "Decimal32";

        public override int Size => 4;

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
