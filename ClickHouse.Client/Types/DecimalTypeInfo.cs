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

        /// <summary>
        /// Size of type in bytes
        /// </summary>
        public virtual int Size => Precision switch
        {
            int p when (p >= 1 && p < 10) => 4,
            int p when (p >= 10 && p < 19) => 8,
            int p when (p >= 19 && p < 39) => 16,
            _ => throw new ArgumentOutOfRangeException(nameof(Precision)),
        };

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

        public override string ToString() => $"{Name}({Precision}, {Scale})";
    }

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

    internal class Decimal128TypeInfo : DecimalTypeInfo
    {
        public Decimal128TypeInfo()
        {
            Precision = 38;
        }
        public override int Size => 16;

        public override string Name => "Decimal128";

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
