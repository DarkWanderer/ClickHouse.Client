using System;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;

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

        public readonly int Exponent { get; }

        public ClickHouseDecimal(BigInteger mantissa, int exponent)
            : this()
        {
            if (MaxPrecision > 0)
            {
                // Normalize() is called as part of Truncate()
                Truncate(ref mantissa, ref exponent, MaxPrecision);
            }
            else
            {
                Normalize(ref mantissa, ref exponent);
            }

            Mantissa = mantissa;
            Exponent = exponent;
        }

        /// <summary>
        /// Removes trailing zeros on the mantissa
        /// </summary>
        private static void Normalize(ref BigInteger mantissa, ref int exponent)
        {
            if (mantissa.IsZero)
            {
                exponent = 0;
            }
            else
            {
                BigInteger remainder = 0;
                while (remainder == 0)
                {
                    var shortened = BigInteger.DivRem(mantissa, 10, out remainder);
                    if (remainder == 0)
                    {
                        mantissa = shortened;
                        exponent++;
                    }
                }
            }
        }

        /// <summary>
        /// Truncate the number to the given precision by removing the least significant digits.
        /// </summary>
        private static void Truncate(ref BigInteger mantissa, ref int exponent, int precision)
        {
            // save some time because the number of digits is not needed to remove trailing zeros
            Normalize(ref mantissa, ref exponent);

            if (precision > -exponent)
                return;

            // remove the least significant digits, as long as the number of digits is higher than the given Precision
            int digits = NumberOfDigits(mantissa);
            int digitsToRemove = Math.Max(digits - precision, 0);
            mantissa /= BigInteger.Pow(10, digitsToRemove);
            exponent += digitsToRemove;

            // normalize again to make sure there are no trailing zeros left
            Normalize(ref mantissa, ref exponent);
        }

        public ClickHouseDecimal Truncate(int precision = 0)
        {
            var mantissa = Mantissa;
            var exponent = Exponent;
            Truncate(ref mantissa, ref exponent, precision);
            return new ClickHouseDecimal(mantissa, exponent);
        }

        public ClickHouseDecimal Floor()
        {
            return Truncate(NumberOfDigits(Mantissa) + Exponent);
        }

        public static int NumberOfDigits(BigInteger value)
        {
            // do not count the sign
            //return (value * value.Sign).ToString().Length;
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
            var exponent = 0;
            double scaleFactor = 1;
            while (Math.Abs(value * scaleFactor - (double)mantissa) > 0)
            {
                exponent -= 1;
                scaleFactor *= 10;
                mantissa = (BigInteger)(value * scaleFactor);
            }
            return new ClickHouseDecimal(mantissa, exponent);
        }

        public static implicit operator ClickHouseDecimal(decimal value)
        {
            var mantissa = (BigInteger)value;
            var exponent = 0;
            decimal scaleFactor = 1;
            while ((decimal)mantissa != value * scaleFactor)
            {
                exponent -= 1;
                scaleFactor *= 10;
                mantissa = (BigInteger)(value * scaleFactor);
            }
            return new ClickHouseDecimal(mantissa, exponent);
        }

        public static explicit operator double(ClickHouseDecimal value)
        {
            return (double)value.Mantissa * Math.Pow(10, value.Exponent);
        }

        public static explicit operator float(ClickHouseDecimal value)
        {
            return Convert.ToSingle((double)value);
        }

        public static explicit operator decimal(ClickHouseDecimal value)
        {
            return (decimal)value.Mantissa * (decimal)Math.Pow(10, value.Exponent);
        }

        public static explicit operator int(ClickHouseDecimal value)
        {
            return (int)(value.Mantissa * BigInteger.Pow(10, value.Exponent));
        }

        public static explicit operator uint(ClickHouseDecimal value)
        {
            return (uint)(value.Mantissa * BigInteger.Pow(10, value.Exponent));
        }

        public static explicit operator long(ClickHouseDecimal value)
        {
            return (long)(value.Mantissa * BigInteger.Pow(10, value.Exponent));
        }

        public static explicit operator ulong(ClickHouseDecimal value)
        {
            return (ulong)(value.Mantissa * BigInteger.Pow(10, value.Exponent));
        }

        public static ClickHouseDecimal operator +(ClickHouseDecimal value)
        {
            return value;
        }

        public static ClickHouseDecimal operator -(ClickHouseDecimal value)
        {
            return new ClickHouseDecimal(-value.Mantissa, value.Exponent);
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
            return Add(left, right);
        }

        public static ClickHouseDecimal operator -(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return Add(left, -right);
        }

        private static ClickHouseDecimal Add(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent > right.Exponent
                ? new ClickHouseDecimal(AlignExponent(left, right) + right.Mantissa, right.Exponent)
                : new ClickHouseDecimal(AlignExponent(right, left) + left.Mantissa, left.Exponent);
        }

        public static ClickHouseDecimal operator *(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return new ClickHouseDecimal(left.Mantissa * right.Mantissa, left.Exponent + right.Exponent);
        }

        public static ClickHouseDecimal operator /(ClickHouseDecimal dividend, ClickHouseDecimal divisor)
        {
            var exponentChange = MaxPrecision - (NumberOfDigits(dividend.Mantissa) - NumberOfDigits(divisor.Mantissa));
            if (exponentChange < 0)
            {
                exponentChange = 0;
            }
            var dividendMantissa = dividend.Mantissa * BigInteger.Pow(10, exponentChange);
            return new ClickHouseDecimal(dividendMantissa / divisor.Mantissa, dividend.Exponent - divisor.Exponent - exponentChange);
        }

        public static ClickHouseDecimal operator %(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left - right * (left / right).Floor();
        }

        public static bool operator ==(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent == right.Exponent && left.Mantissa == right.Mantissa;
        }

        public static bool operator !=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent != right.Exponent || left.Mantissa != right.Mantissa;
        }

        public static bool operator <(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent > right.Exponent ? AlignExponent(left, right) < right.Mantissa : left.Mantissa < AlignExponent(right, left);
        }

        public static bool operator >(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent > right.Exponent ? AlignExponent(left, right) > right.Mantissa : left.Mantissa > AlignExponent(right, left);
        }

        public static bool operator <=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent > right.Exponent ? AlignExponent(left, right) <= right.Mantissa : left.Mantissa <= AlignExponent(right, left);
        }

        public static bool operator >=(ClickHouseDecimal left, ClickHouseDecimal right)
        {
            return left.Exponent > right.Exponent ? AlignExponent(left, right) >= right.Mantissa : left.Mantissa >= AlignExponent(right, left);
        }

        /// <summary>
        /// Returns the mantissa of value, aligned to the exponent of reference.
        /// Assumes the exponent of value is larger than of reference.
        /// </summary>
        private static BigInteger AlignExponent(ClickHouseDecimal value, ClickHouseDecimal reference)
        {
            return value.Mantissa * BigInteger.Pow(10, value.Exponent - reference.Exponent);
        }

        public static ClickHouseDecimal Exp(double exponent)
        {
            var tmp = (ClickHouseDecimal)1;
            while (Math.Abs(exponent) > 100)
            {
                var diff = exponent > 0 ? 100 : -100;
                tmp *= Math.Exp(diff);
                exponent -= diff;
            }
            return tmp * Math.Exp(exponent);
        }

        public static ClickHouseDecimal Pow(double basis, double exponent)
        {
            var tmp = (ClickHouseDecimal)1;
            while (Math.Abs(exponent) > 100)
            {
                var diff = exponent > 0 ? 100 : -100;
                tmp *= Math.Pow(basis, diff);
                exponent -= diff;
            }
            return tmp * Math.Pow(basis, exponent);
        }

        public bool Equals(ClickHouseDecimal other)
        {
            return other.Mantissa.Equals(Mantissa) && other.Exponent == Exponent;
        }

        public override bool Equals(object obj) => CompareTo(obj) == 0;

        public override int GetHashCode()
        {
            unchecked
            {
                return (Mantissa.GetHashCode() * 397) ^ Exponent;
            }
        }

        public int CompareTo(object obj)
        {
            return obj is ClickHouseDecimal cbi ? CompareTo(cbi) : 1;
        }

        public int CompareTo(ClickHouseDecimal other)
        {
            return this < other ? -1 : (this > other ? 1 : 0);
        }

        public string ToString(string format, IFormatProvider formatProvider) => ToString(formatProvider);

        public string ToString(IFormatProvider formatProvider)
        {
            var cultureInfo = formatProvider is CultureInfo ci ? ci : CultureInfo.InvariantCulture;
            var numberFormat = cultureInfo.NumberFormat;
            var builder = new StringBuilder();


            if (Exponent < 0)
            {
                var mantissa = Mantissa;
                if (mantissa < 0)
                {
                    builder.Append(numberFormat.NegativeSign);
                    mantissa = -mantissa;
                }
                var factor = BigInteger.Pow(10, -Exponent);
                var wholePart = mantissa / factor;
                var fractionalPart = mantissa - (wholePart * factor);

                builder.Append(wholePart.ToString(formatProvider));
                builder.Append(numberFormat.NumberDecimalSeparator);
                builder.Append(fractionalPart.ToString(formatProvider).PadLeft(-Exponent, '0'));
            }
            else
            {
                builder.Append(Mantissa * BigInteger.Pow(10, Exponent));
            }

            return builder.ToString();
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
