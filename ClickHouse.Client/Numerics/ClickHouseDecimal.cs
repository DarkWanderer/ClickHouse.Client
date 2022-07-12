using System;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Numerics
{
    /// <summary>
    /// Arbitrary precision decimal.
    /// All operations are exact, except for division. Division never determines more digits than the given precision.
    /// Based on: https://gist.github.com/JcBernack/0b4eef59ca97ee931a2f45542b9ff06d
    /// Based on https://stackoverflow.com/a/4524254
    /// Original Author: Jan Christoph Bernack (contact: jc.bernack at gmail.com)
    /// </summary>
    public readonly struct ClickHouseDecimal
        : IComparable, IComparable<ClickHouseDecimal>, IFormattable, IConvertible, IEquatable<ClickHouseDecimal>, IComparable<decimal>
    {
        /// <summary>
        /// Sets the global maximum precision of division operations.
        /// </summary>
        public static int MaxPrecision = 50;

        public readonly BigInteger Mantissa { get; }

        public readonly ushort Scale { get; }

        public static ClickHouseDecimal Zero => new (0, 0);

        public static ClickHouseDecimal One => new (1, 0);

        public int Sign => Mantissa.Sign;

        public ClickHouseDecimal(decimal value)
            : this()
        {
            // Slightly wasteful, but seems to be the cheapest way to get scale
            var parts = decimal.GetBits(value);
            ushort scale = (ushort)((parts[3] >> 16) & 0x7F);

            value *= MathUtils.ToPower(10m, scale);

            var mantissa = new BigInteger(value);

            if (MaxPrecision > 0)
            {
                // Normalize() is called as part of Truncate()
                Truncate(ref mantissa, ref scale, MaxPrecision);
            }

            Mantissa = mantissa;
            Scale = scale;
        }

        public ClickHouseDecimal(BigInteger mantissa, ushort scale)
            : this()
        {
            if (MaxPrecision > 0)
            {
                // Normalize() is called as part of Truncate()
                Truncate(ref mantissa, ref scale, MaxPrecision);
            }
            else
            {
                Normalize(ref mantissa, ref scale);
            }

            Mantissa = mantissa;
            Scale = scale;
        }

        /// <summary>
        /// Removes trailing zeros on the mantissa
        /// </summary>
        private static void Normalize(ref BigInteger mantissa, ref ushort scale)
        {
            if (mantissa.IsZero)
            {
                scale = 0;
            }
            else
            {
                BigInteger remainder = 0;
                while (remainder == 0 && scale > 0)
                {
                    var shortened = BigInteger.DivRem(mantissa, 10, out remainder);
                    if (remainder == 0)
                    {
                        mantissa = shortened;
                        scale--;
                    }
                }
            }
        }

        /// <summary>
        /// Truncate the number to the given precision by removing the least significant digits.
        /// </summary>
        private static void Truncate(ref BigInteger mantissa, ref ushort scale, int precision)
        {
            // remove the least significant digits, as long as the number of digits is higher than the given Precision
            int digits = NumberOfDigits(mantissa);
            ushort digitsToRemove = (ushort)Math.Max(digits - precision, 0);
            digitsToRemove = Math.Min(digitsToRemove, scale);
            mantissa /= BigInteger.Pow(10, digitsToRemove);
            scale -= digitsToRemove;
        }

        public ClickHouseDecimal Truncate(int precision = 0)
        {
            var mantissa = Mantissa;
            var scale = Scale;
            Truncate(ref mantissa, ref scale, precision);
            return new ClickHouseDecimal(mantissa, scale);
        }

        public ClickHouseDecimal Floor()
        {
            return Truncate(NumberOfDigits(Mantissa) - Scale);
        }

        public static int NumberOfDigits(BigInteger value)
        {
            // faster version
            return (int)Math.Ceiling(BigInteger.Log10(value * value.Sign));
        }

        public static implicit operator ClickHouseDecimal(int value)
        {
            return new ClickHouseDecimal(value, 0);
        }

        public static implicit operator ClickHouseDecimal(double value)
        {
            var mantissa = (BigInteger)value;
            ushort scale = 0;
            double scaleFactor = 1;
            while (Math.Abs((value * scaleFactor) - (double)mantissa) > 0)
            {
                scale += 1;
                scaleFactor *= 10;
                mantissa = (BigInteger)(value * scaleFactor);
            }
            return new ClickHouseDecimal(mantissa, scale);
        }

        public static implicit operator ClickHouseDecimal(decimal value)
        {
            return new ClickHouseDecimal(value);
        }

        public static explicit operator double(ClickHouseDecimal value)
        {
            return (double)value.Mantissa * Math.Pow(10, value.Scale);
        }

        public static explicit operator float(ClickHouseDecimal value)
        {
            return Convert.ToSingle((double)value);
        }

        public static explicit operator decimal(ClickHouseDecimal value)
        {
            var mantissa = value.Mantissa;
            var scale = value.Scale;

            Truncate(ref mantissa, ref scale, 28);

            bool sign = mantissa < 0;
            if (sign)
            {
                mantissa = BigInteger.Negate(mantissa);
            }

            var data = new byte[3 * sizeof(int)];
            mantissa.ToByteArray().CopyTo(data, 0);
            int part0 = BitConverter.ToInt32(data, 0);
            int part1 = BitConverter.ToInt32(data, 4);
            int part2 = BitConverter.ToInt32(data, 8);

            var result = new decimal(part0, part1, part2, sign, (byte)scale);
            return result;
        }

        public static explicit operator int(ClickHouseDecimal value)
        {
            return (int)(value.Mantissa * BigInteger.Pow(10, value.Scale));
        }

        public static explicit operator uint(ClickHouseDecimal value)
        {
            return (uint)(value.Mantissa * BigInteger.Pow(10, value.Scale));
        }

        public static explicit operator long(ClickHouseDecimal value)
        {
            return (long)(value.Mantissa * BigInteger.Pow(10, value.Scale));
        }

        public static explicit operator ulong(ClickHouseDecimal value)
        {
            return (ulong)(value.Mantissa * BigInteger.Pow(10, value.Scale));
        }

        public static ClickHouseDecimal operator +(ClickHouseDecimal value)
        {
            return value;
        }

        public static ClickHouseDecimal operator -(ClickHouseDecimal value)
        {
            return new ClickHouseDecimal(-value.Mantissa, value.Scale);
        }

        public static ClickHouseDecimal operator ++(ClickHouseDecimal value)
        {
            return value + 1;
        }

        public static ClickHouseDecimal operator --(ClickHouseDecimal value)
        {
            return value - 1;
        }

        public static ClickHouseDecimal operator +(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            var scale = Math.Max(left.Scale, right.Scale);
            var left_mantissa = ToScale(left, scale);
            var right_mantissa = ToScale(right, scale);

            return new ClickHouseDecimal(left_mantissa + right_mantissa, scale);
        }

        public static ClickHouseDecimal operator -(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            var scale = Math.Max(left.Scale, right.Scale);
            var left_mantissa = ToScale(left, scale);
            var right_mantissa = ToScale(right, scale);

            return new ClickHouseDecimal(left_mantissa - right_mantissa, scale);
        }

        public static ClickHouseDecimal operator *(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return new ClickHouseDecimal(left.Mantissa * right.Mantissa, (ushort)(left.Scale + right.Scale));
        }

        public static ClickHouseDecimal operator /(ClickHouseDecimal dividend, ClickHouseDecimal divisor)
        {
            var scale = Math.Max(dividend.Scale, divisor.Scale);
            var dividend_mantissa = ToScale(dividend, scale);
            var divisor_mantissa = ToScale(divisor, scale);

            return new ClickHouseDecimal(dividend_mantissa / divisor_mantissa, scale);
        }

        public static ClickHouseDecimal operator %(ClickHouseDecimal dividend, ClickHouseDecimal divisor)
        {
            var scale = Math.Max(dividend.Scale, divisor.Scale);
            var dividend_mantissa = ToScale(dividend, scale);
            var divisor_mantissa = ToScale(divisor, scale);

            return new ClickHouseDecimal(dividend_mantissa % divisor_mantissa, scale);
        }

        public static bool operator ==(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) == 0;
        }

        public static bool operator !=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) != 0;
        }

        public static bool operator <(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.CompareTo(right) >= 0;
        }

        public static BigInteger ToScale(ClickHouseDecimal value, ushort scale)
        {
            if (scale == value.Scale)
                return value.Mantissa;
            if (scale < value.Scale)
                throw new ArgumentException("Cannot adjust mantissa to lower scale", nameof(scale));
            return value.Mantissa * BigInteger.Pow(10, scale - value.Scale);
        }

        public static ClickHouseDecimal Exp(double scale)
        {
            var tmp = (ClickHouseDecimal)1;
            while (Math.Abs(scale) > 100)
            {
                var diff = scale > 0 ? 100 : -100;
                tmp *= Math.Exp(diff);
                scale -= diff;
            }
            return tmp * Math.Exp(scale);
        }

        public static ClickHouseDecimal Pow(double basis, double scale)
        {
            var tmp = (ClickHouseDecimal)1;
            while (Math.Abs(scale) > 100)
            {
                var diff = scale > 0 ? 100 : -100;
                tmp *= Math.Pow(basis, diff);
                scale -= diff;
            }
            return tmp * Math.Pow(basis, scale);
        }

        public bool Equals(ClickHouseDecimal other)
        {
            return other.Mantissa.Equals(Mantissa) && other.Scale == Scale;
        }

        public override bool Equals(object obj) => CompareTo(obj) == 0;

        public override int GetHashCode()
        {
            unchecked
            {
                return (Mantissa.GetHashCode() * 397) ^ Scale;
            }
        }

        public int CompareTo(object obj)
        {
            return obj is ClickHouseDecimal cbi ? CompareTo(cbi) : 1;
        }

        public int CompareTo(ClickHouseDecimal other)
        {
            var maxScale = Math.Max(Scale, other.Scale);
            var left_mantissa = ToScale(this, maxScale);
            var right_mantissa = ToScale(other, maxScale);

            return left_mantissa.CompareTo(right_mantissa);
        }

        public string ToString(string format, IFormatProvider formatProvider) => ToString(formatProvider);

        public string ToString(IFormatProvider provider)
        {
            provider ??= CultureInfo.CurrentCulture;
            var numberFormat = (NumberFormatInfo)provider.GetFormat(typeof(NumberFormatInfo));
            var builder = new StringBuilder();

            var mantissa = Mantissa;
            if (mantissa < 0)
            {
                builder.Append(numberFormat.NegativeSign);
                mantissa = BigInteger.Negate(mantissa);
            }

            if (Scale > 0)
            {
                var factor = BigInteger.Pow(10, Scale);
                var wholePart = mantissa / factor;
                var fractionalPart = mantissa - (wholePart * factor);
                builder.Append(wholePart.ToString(provider));
                builder.Append(numberFormat.NumberDecimalSeparator);
                builder.Append(fractionalPart.ToString(provider).PadLeft(Scale, '0'));
            }
            else
            {
                builder.Append(mantissa.ToString(provider));
            }

            return builder.ToString();
        }

        public static ClickHouseDecimal Parse(string input) => Parse(input, CultureInfo.CurrentCulture);

        public static ClickHouseDecimal Parse(string input, IFormatProvider provider)
        {
            var numberFormat = (NumberFormatInfo)provider.GetFormat(typeof(NumberFormatInfo));

            if (string.IsNullOrWhiteSpace(input))
            {
                return Zero;
            }
            input = input.Trim();

            string mantissaPart = input;
            string fractionalPart = string.Empty;

            int separatorIndex = input.IndexOf(numberFormat.NumberDecimalSeparator, StringComparison.InvariantCultureIgnoreCase);
            if (separatorIndex > 0)
            {
                fractionalPart = input.Substring(separatorIndex + 1);
                mantissaPart = input.Replace(numberFormat.NumberDecimalSeparator, string.Empty);
            }
            var mantissa = BigInteger.Parse(mantissaPart, NumberStyles.Any, provider);
            var scale = (ushort)fractionalPart.Length;

            return new ClickHouseDecimal(mantissa, scale);
        }

        public override string ToString() => ToString(null, CultureInfo.CurrentCulture);

        public TypeCode GetTypeCode() => TypeCode.Object;

        public bool ToBoolean(IFormatProvider provider) => !Mantissa.IsZero;

        public char ToChar(IFormatProvider provider) => (char)(int)this;

        public sbyte ToSByte(IFormatProvider provider) => (sbyte)(int)this;

        public byte ToByte(IFormatProvider provider) => (byte)(int)this;

        public short ToInt16(IFormatProvider provider) => (short)(int)this;

        public ushort ToUInt16(IFormatProvider provider) => (ushort)(uint)this;

        public int ToInt32(IFormatProvider provider) => (short)(int)this;

        public uint ToUInt32(IFormatProvider provider) => (uint)this;

        public long ToInt64(IFormatProvider provider) => (long)this;

        public ulong ToUInt64(IFormatProvider provider) => (ulong)this;

        public float ToSingle(IFormatProvider provider) => (float)this;

        public double ToDouble(IFormatProvider provider) => (double)this;

        public decimal ToDecimal(IFormatProvider provider) => (decimal)this;

        public DateTime ToDateTime(IFormatProvider provider) => throw new NotSupportedException();

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            if (conversionType == typeof(int))
                return ToInt32(provider);
            throw new NotSupportedException();
        }

        public int CompareTo(decimal other) => CompareTo((ClickHouseDecimal)other);
    }
}
