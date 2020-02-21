using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal128Type : DecimalType
    {
        public Decimal128Type()
        {
            Precision = 38;
        }

        public override int Size => 16;

        public override string Name => "Decimal128";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new Decimal128Type
            {
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets()),
            };
        }

        public override string ToString() => $"{Name}({Scale})";
    }
}
