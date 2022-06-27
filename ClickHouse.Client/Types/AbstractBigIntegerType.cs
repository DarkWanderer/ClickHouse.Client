using System;
using System.Numerics;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal abstract class AbstractBigIntegerType : IntegerType
    {
        public virtual int Size { get; }

        public override Type FrameworkType => typeof(BigInteger);

        public override object Read(ExtendedBinaryReader reader) => new BigInteger(reader.ReadBytes(Size));

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
                _ => new BigInteger(Convert.ToInt64(value))
            };

            byte[] bigIntBytes = bigInt.ToByteArray();
            byte[] decimalBytes = new byte[Size];

            if (bigIntBytes.Length > Size)
                throw new OverflowException();

            bigIntBytes.CopyTo(decimalBytes, 0);

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
