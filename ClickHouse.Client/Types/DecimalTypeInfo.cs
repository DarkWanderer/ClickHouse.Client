using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class DecimalTypeInfo : ParameterizedTypeInfo
    {
        public virtual int Precision { get; set; }

        public int Scale { get; set; }

        public override string Name => "Decimal";

        public override ClickHouseDataType DataType => ClickHouseDataType.Decimal;

        public override Type EquivalentType => typeof(decimal);

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var parameters = typeName.Substring(Name.Length).TrimRoundBrackets().Split(',');

            return new DecimalTypeInfo
            {
                Precision = int.Parse(parameters[0]),
                Scale = int.Parse(parameters[1]),
            };
        }

        public override string ToString() => $"Decimal({Precision}, {Scale})";
    }

    internal class Decimal32TypeInfo : DecimalTypeInfo
    {
        public override string Name => "Decimal32";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new DecimalTypeInfo
            {
                Precision = 9,
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }
    }
    internal class Decimal64TypeInfo : DecimalTypeInfo
    {
        public override string Name => "Decimal64";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new DecimalTypeInfo
            {
                Precision = 18,
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

    }

    internal class Decimal128TypeInfo : DecimalTypeInfo
    {
        public override string Name => "Decimal128";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new DecimalTypeInfo
            {
                Precision = 38,
                Scale = int.Parse(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

    }
}
