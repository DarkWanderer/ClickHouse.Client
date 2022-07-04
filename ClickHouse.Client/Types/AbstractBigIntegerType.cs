using System;
using System.Globalization;
using System.Numerics;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal abstract class AbstractBigIntegerType : IntegerType
    {
        public virtual int Size { get; }

        public override Type FrameworkType => typeof(BigInteger);

        public override object Read(ExtendedBinaryReader reader)
        {
            if (Signed)
                return new BigInteger(reader.ReadBytes(Size));

            var data = new byte[Size + 1];
            for (int i = 0; i < Size; i++)
                data[i] = reader.ReadByte();
            data[Size] = 0;
            return new BigInteger(data);
        }

        public abstract override string ToString();

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var bigInt = value switch
            {
                BigInteger bi => bi,
                decimal dl => new BigInteger(dl),
                double d => new BigInteger(d),
                float f => new BigInteger(f),
                int i => new BigInteger(i),
                uint ui => new BigInteger(ui),
                long l => new BigInteger(l),
                ulong ul => new BigInteger(ul),
                _ => new BigInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture))
            };

            if (bigInt < 0 && !Signed)
                throw new ArgumentException("Cannot convert negative BigInteger to UInt");

            byte[] bigIntBytes = bigInt.ToByteArray();
            byte[] decimalBytes = new byte[Size];

            var lengthToCopy = bigIntBytes.Length;
            if (!Signed && bigIntBytes[bigIntBytes.Length - 1] == 0)
                lengthToCopy = bigIntBytes.Length - 1;

            if (lengthToCopy > Size)
                throw new OverflowException($"Got {lengthToCopy} bytes, {Size} expected");

            Array.Copy(bigIntBytes, decimalBytes, lengthToCopy);

            // If a negative BigInteger is not long enough to fill the whole buffer,
            // the remainder needs to be filled with 0xFF
            if (bigInt < 0)
            {
                for (int i = bigIntBytes.Length; i < Size; i++)
                    decimalBytes[i] = 0xFF;
            }
            writer.Write(decimalBytes);
        }
    }
}
