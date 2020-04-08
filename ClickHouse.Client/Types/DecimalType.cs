using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class DecimalType : ParameterizedType
    {
        public virtual int Precision { get; set; }

        public int Scale { get; set; }

        public override string Name => "Decimal";

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Decimal;

        /// <summary>
        /// Gets size of type in bytes
        /// </summary>
        public virtual int Size => GetSizeFromPrecision(Precision);

        private int GetSizeFromPrecision(int precision) => precision switch
        {
            int p when p >= 1 && p < 10 => 4,
            int p when p >= 10 && p < 19 => 8,
            int p when p >= 19 && p < 39 => 16,
            _ => throw new ArgumentOutOfRangeException(nameof(Precision)),
        };

        public override Type FrameworkType => typeof(decimal);

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
            {
                throw new ArgumentException(nameof(typeName));
            }

            var parameters = typeName.Substring(Name.Length).TrimRoundBrackets().Split(',');
            var precision = int.Parse(parameters[0]);
            var scale = int.Parse(parameters[1]);

            var size = GetSizeFromPrecision(precision);

            switch (size)
            {
                case 4:
                    return new Decimal32Type { Precision = precision, Scale = scale };
                case 8:
                    return new Decimal64Type { Precision = precision, Scale = scale };
                case 16:
                    return new Decimal128Type { Precision = precision, Scale = scale };
                default:
                    return new DecimalType { Precision = precision, Scale = scale };
            }
        }

        public override string ToString() => $"{Name}({Precision}, {Scale})";
    }
}
