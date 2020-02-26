using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal64Type : DecimalType
    {
        public Decimal64Type()
        {
            Precision = 18;
        }

        public override int Size => 8;

        public override string Name => "Decimal64";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
            {
                throw new ArgumentException(nameof(typeName));
            }

            return new Decimal64Type
            {
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets()),
            };
        }

        public override string ToString() => $"{Name}({Scale})";
    }
}
