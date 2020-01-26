using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class Decimal32Type : DecimalType
    {
        public Decimal32Type()
        {
            Precision = 9;
        }

        public override string Name => "Decimal32";

        public override int Size => 4;

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new Decimal32Type
            {
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }
        public override string ToString() => $"{Name}({Scale})";
    }
}
