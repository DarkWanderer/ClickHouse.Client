using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class DecimalTypeInfo : ParameterizedTypeInfo
    {
        public virtual int Precision { get; set; }

        public int Scale { get; set; }

        public override string Name => "Decimal";

        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Decimal;

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
}
