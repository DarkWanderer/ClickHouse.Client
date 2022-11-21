using System;
using System.Globalization;
using System.Numerics;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

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
        }
    }

    public override string Name => "Decimal";

    /// <summary>
    /// Gets size of type in bytes
    /// </summary>
    public virtual int Size => GetSizeFromPrecision(Precision);

    public override Type FrameworkType => typeof(ClickHouseDecimal);

    public ClickHouseDecimal MaxValue => new(BigInteger.Pow(10, Precision) - 1, Scale);

    public ClickHouseDecimal MinValue => new(1 - BigInteger.Pow(10, Precision), Scale);

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var precision = int.Parse(node.ChildNodes[0].Value, CultureInfo.InvariantCulture);
        var scale = int.Parse(node.ChildNodes[1].Value, CultureInfo.InvariantCulture);

        var size = GetSizeFromPrecision(precision);

        switch (size)
        {
            case 4:
                return new Decimal32Type { Precision = precision, Scale = scale };
            case 8:
                return new Decimal64Type { Precision = precision, Scale = scale };
            case 16:
                return new Decimal128Type { Precision = precision, Scale = scale };
            case 32:
                return new Decimal256Type { Precision = precision, Scale = scale };
            default:
                return new DecimalType { Precision = precision, Scale = scale };
        }
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        // ClickHouse value represented as decimal
        // Needs to be divided by Exponent to get actual value
        BigInteger mantissa;
        switch (Size)
        {
            case 4:
                mantissa = reader.ReadInt32();
                break;
            case 8:
                mantissa = reader.ReadInt64();
                break;
            default:
                mantissa = new BigInteger(reader.ReadBytes(Size));
                break;
        }
        return new ClickHouseDecimal(mantissa, Scale);
    }

    public override string ToString() => $"{Name}({Precision}, {Scale})";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        try
        {
            ClickHouseDecimal @decimal = value is ClickHouseDecimal chd ? chd : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            var mantissa = ClickHouseDecimal.ScaleMantissa(@decimal, Scale);
            WriteBigInteger(writer, mantissa);
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, $"Value cannot be represented");
        }
    }

    private static int GetSizeFromPrecision(int precision) => precision switch
    {
        int p when p >= 1 && p <= 9 => 4,
        int p when p >= 10 && p <= 18 => 8,
        int p when p >= 19 && p <= 38 => 16,
        int p when p >= 39 && p <= 76 => 32,
        _ => throw new ArgumentOutOfRangeException(nameof(precision)),
    };

    private void WriteBigInteger(ExtendedBinaryWriter writer, BigInteger value)
    {
        byte[] bigIntBytes = value.ToByteArray();
        byte[] decimalBytes = new byte[Size];

        if (bigIntBytes.Length > Size)
            throw new OverflowException($"Trying to write {bigIntBytes.Length} bytes, at most {Size} expected");

        bigIntBytes.CopyTo(decimalBytes, 0);

        // If a negative BigInteger is not long enough to fill the whole buffer,
        // the remainder needs to be filled with 0xFF
        if (value.Sign < 0)
        {
            for (int i = bigIntBytes.Length; i < Size; i++)
                decimalBytes[i] = 0xFF;
        }
        writer.Write(decimalBytes);
    }
}
