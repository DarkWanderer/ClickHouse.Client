using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal128TypeInfo : DecimalTypeInfo
    {
        public Decimal128TypeInfo()
        {
            Precision = 38;
        }
        public override int Size => 16;

        public override string Name => "Decimal128";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
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
