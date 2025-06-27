using System;
using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;

namespace ClickHouse.Client.Numerics;

[StructLayout(LayoutKind.Sequential)]
public readonly struct ClickHouseInt128
    :
#if NET8_0_OR_GREATER
        ISignedNumber<ClickHouseInt128>,
 IMinMaxValue<ClickHouseInt128>,
// IBinaryInteger<ClickHouseInt128>,
// IUtf8SpanFormattable,
// IBinaryIntegerParseAndFormatInfo<ClickHouseInt128>
#endif
    IEquatable<ClickHouseInt128>,
    IComparable<ClickHouseInt128>
{
#if BIGENDIAN
        private readonly ulong upper;
        private readonly ulong lower;
#else
    private readonly ulong lower;
    private readonly ulong upper;
#endif

#if NET8_0_OR_GREATER
    public ClickHouseInt128(Int128 value)
    {

    }
#endif

    public ClickHouseInt128(ulong lower)
    {
        this.upper = 0;
        this.lower = lower;
    }

    public ClickHouseInt128(ulong upper, ulong lower)
    {
        this.upper = upper;
        this.lower = lower;
    }

    // Very slow and unoptimized constructor - used only for testing/comparison purposes
    public ClickHouseInt128(BigInteger value)
    {
        var bytes = value.ToByteArray();
        var int128bytes = new byte[16];
        if (value.Sign < 0)
        {
            int128bytes.AsSpan().Fill(byte.MaxValue);
        }
        else
        {
            int128bytes.AsSpan().Clear();
        }

        if (bytes.Length > 16)
            throw new OverflowException("Value is too large to fit in ClickHouseInt128.");

        bytes.CopyTo(int128bytes.AsSpan());

        lower = BitConverter.ToUInt64(int128bytes, 0);
        upper = BitConverter.ToUInt64(int128bytes, 8);
    }

    public static ClickHouseInt128 NegativeOne => new(ulong.MaxValue, ulong.MaxValue);

    public static ClickHouseInt128 One => new(1);

    public static int Radix => 2;

    public static ClickHouseInt128 Zero => new(0);

    public static ClickHouseInt128 AdditiveIdentity => default;

    public static ClickHouseInt128 MultiplicativeIdentity => One;

    internal ulong Lower => lower;

    internal ulong Upper => upper;

    public static ClickHouseInt128 MaxValue => new(long.MaxValue, ulong.MaxValue);

    public static ClickHouseInt128 MinValue => new(0x8000_0000_0000_0000, 0);

    public static ClickHouseInt128 Abs(ClickHouseInt128 value) => throw new NotImplementedException();

    public static bool IsCanonical(ClickHouseInt128 value) => true;

    public static bool IsComplexNumber(ClickHouseInt128 value) => false;

    public static bool IsEvenInteger(ClickHouseInt128 value) => (value.lower & 1) == 0;

    public static bool IsFinite(ClickHouseInt128 value) => true;

    public static bool IsImaginaryNumber(ClickHouseInt128 value) => false;

    public static bool IsInfinity(ClickHouseInt128 value) => false;

    public static bool IsInteger(ClickHouseInt128 value) => true;

    public static bool IsNaN(ClickHouseInt128 value) => false;

    public static bool IsNegative(ClickHouseInt128 value) => (long)value.Upper < 0;

    public static bool IsNegativeInfinity(ClickHouseInt128 value) => false;

    public static bool IsNormal(ClickHouseInt128 value) => throw new NotImplementedException();

    public static bool IsOddInteger(ClickHouseInt128 value) => (value.lower & 1) != 0;

    public static bool IsPositive(ClickHouseInt128 value) => (long)value.Upper > 0;

    public static bool IsPositiveInfinity(ClickHouseInt128 value) => false;

    public static bool IsRealNumber(ClickHouseInt128 value) => true;

    public static bool IsSubnormal(ClickHouseInt128 value) => false;

    public static bool IsZero(ClickHouseInt128 value) => value.Upper == 0 && value.Lower == 0;

    public static ClickHouseInt128 MaxMagnitude(ClickHouseInt128 x, ClickHouseInt128 y) => throw new NotImplementedException();

    public static ClickHouseInt128 MaxMagnitudeNumber(ClickHouseInt128 x, ClickHouseInt128 y) => throw new NotImplementedException();

    public static ClickHouseInt128 MinMagnitude(ClickHouseInt128 x, ClickHouseInt128 y) => throw new NotImplementedException();

    public static ClickHouseInt128 MinMagnitudeNumber(ClickHouseInt128 x, ClickHouseInt128 y) => throw new NotImplementedException();

    public static ClickHouseInt128 Parse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider provider) => throw new NotImplementedException();

    public static ClickHouseInt128 Parse(string s, NumberStyles style, IFormatProvider provider) => throw new NotImplementedException();

    public static ClickHouseInt128 Parse(ReadOnlySpan<char> s, IFormatProvider provider) => throw new NotImplementedException();

    public static ClickHouseInt128 Parse(string s, IFormatProvider provider) => throw new NotImplementedException();

#if NET8_0_OR_GREATER
    public static bool TryConvertFromChecked<TOther>(TOther value, out ClickHouseInt128 result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

    public static bool TryConvertFromSaturating<TOther>(TOther value, out ClickHouseInt128 result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

    public static bool TryConvertFromTruncating<TOther>(TOther value, out ClickHouseInt128 result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

    public static bool TryConvertToChecked<TOther>(ClickHouseInt128 value, out TOther result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

    public static bool TryConvertToSaturating<TOther>(ClickHouseInt128 value, out TOther result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

    public static bool TryConvertToTruncating<TOther>(ClickHouseInt128 value, out TOther result)
        where TOther : INumberBase<TOther>
        => throw new NotImplementedException();

#endif
    public static bool TryParse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider provider, out ClickHouseInt128 result) => throw new NotImplementedException();

    public static bool TryParse(string s, NumberStyles style, IFormatProvider provider, out ClickHouseInt128 result) => throw new NotImplementedException();

    public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider provider, out ClickHouseInt128 result) => throw new NotImplementedException();

    public static bool TryParse(string s, IFormatProvider provider, out ClickHouseInt128 result) => throw new NotImplementedException();

    public string ToString(string format, IFormatProvider formatProvider) => throw new NotImplementedException();

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider provider) => throw new NotImplementedException();

    public static ClickHouseInt128 operator +(ClickHouseInt128 value) => value;

    public static ClickHouseInt128 operator +(ClickHouseInt128 left, ClickHouseInt128 right)
    {
        ulong lower = left.lower + right.lower;
        // Carry 1 if sum overflows
        ulong carry = (lower < left.lower) ? 1UL : 0UL;

        ulong upper = left.upper + right.upper + carry;
        return new ClickHouseInt128(upper, lower);
    }

    public static ClickHouseInt128 operator -(ClickHouseInt128 value) => Zero - value;

    public static ClickHouseInt128 operator -(ClickHouseInt128 left, ClickHouseInt128 right)
    {
        ulong lower = left.lower - right.lower;
        // Borrow 1 if subtraction underflows
        ulong borrow = (lower > left.lower) ? 1UL : 0UL;

        ulong upper = left.upper - right.upper - borrow;
        return new ClickHouseInt128(upper, lower);
    }

    public static ClickHouseInt128 operator ++(ClickHouseInt128 value) => value + One;

    public static ClickHouseInt128 operator --(ClickHouseInt128 value) => value - One;

    public static ClickHouseInt128 operator *(ClickHouseInt128 left, ClickHouseInt128 right) => throw new NotImplementedException();

    public static ClickHouseInt128 operator /(ClickHouseInt128 left, ClickHouseInt128 right) => throw new NotImplementedException();

    public static bool operator ==(ClickHouseInt128 left, ClickHouseInt128 right) => left.CompareTo(right) == 0;

    public static bool operator !=(ClickHouseInt128 left, ClickHouseInt128 right) => left.CompareTo(right) != 0;

    public static explicit operator BigInteger(ClickHouseInt128 value)
    {
        var int128bytes = new byte[16];
        BitConverter.GetBytes(value.lower).CopyTo(int128bytes.AsSpan(0));
        BitConverter.GetBytes(value.upper).CopyTo(int128bytes.AsSpan(8));
        return new BigInteger(int128bytes);
    }

    public bool Equals(ClickHouseInt128 other) => CompareTo(other) == 0;

    public int CompareTo(ClickHouseInt128 other)
    {
        var comparison1 = Upper.CompareTo(other.Upper);
        if (comparison1 != 0)
            return comparison1;
        return Lower.CompareTo(other.Lower);
    }

    public override bool Equals(object obj) => obj is ClickHouseInt128 other && CompareTo(other) == 0;
}
