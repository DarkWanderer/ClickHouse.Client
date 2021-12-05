using System;
using System.Numerics;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class DecimalType : ParameterizedType
    {
        private int scale;

        public virtual int Precision { get; set; }

        /// <summary>
        /// Gets or sets the decimal 'scale' (precision) in ClickHouse
        /// </summary>
        public int Scale
        {
            get => scale;
            set
            {
                scale = value;
                Exponent = MathUtils.ToPower(10, value);
            }
        }

        /// <summary>
        /// Gets decimal exponent value based on Scale
        /// </summary>
        public long Exponent { get; private set; }

        public override string Name => "Decimal";

        /// <summary>
        /// Gets size of type in bytes
        /// </summary>
        public virtual int Size => GetSizeFromPrecision(Precision);

        public override Type FrameworkType => typeof(decimal);

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            var precision = int.Parse(node.ChildNodes[0].Value);
            var scale = int.Parse(node.ChildNodes[1].Value);

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

        public override object Read(ExtendedBinaryReader reader)
        {
            switch (Size)
            {
                case 4:
                    return (decimal)reader.ReadInt32() / Exponent;
                case 8:
                    return (decimal)reader.ReadInt64() / Exponent;
                default:
                    var bigInt = new BigInteger(reader.ReadBytes(Size));
                    return (decimal)bigInt / Exponent;
            }
        }

        public override string ToString() => $"{Name}({Precision}, {Scale})";

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            try
            {
                decimal multipliedValue = Convert.ToDecimal(value) * Exponent;
                switch (Size)
                {
                    case 4:
                        writer.Write((int)multipliedValue);
                        break;
                    case 8:
                        writer.Write((long)multipliedValue);
                        break;
                    default:
                        WriteLargeDecimal(writer, multipliedValue);
                        break;
                }
            }
            catch (OverflowException)
            {
                throw new ArgumentOutOfRangeException("value", value, $"Value cannot be represented as {this}");
            }
        }

        private int GetSizeFromPrecision(int precision) => precision switch
        {
            int p when p >= 1 && p < 10 => 4,
            int p when p >= 10 && p < 19 => 8,
            int p when p >= 19 && p < 39 => 16,
            _ => throw new ArgumentOutOfRangeException(nameof(Precision)),
        };

        private void WriteLargeDecimal(ExtendedBinaryWriter writer, decimal value)
        {
            var bigInt = new BigInteger(value);
            byte[] bigIntBytes = bigInt.ToByteArray();
            byte[] decimalBytes = new byte[Size];

            if (bigIntBytes.Length > Size)
                throw new OverflowException();

            bigIntBytes.CopyTo(decimalBytes, 0);

            // If a negative BigInteger is not long enough to fill the whole buffer, the remainder needs to be filled with 0xFF
            if (bigInt < 0)
            {
                for (int i = bigIntBytes.Length; i < Size; i++)
                    decimalBytes[i] = 0xFF;
            }
            writer.Write(decimalBytes);
        }
    }
}
