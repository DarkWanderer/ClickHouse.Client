using System;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class BinaryStreamReader : IStreamDataReader, IDisposable
    {
        private readonly ExtendedBinaryReader reader;

        public BinaryStreamReader(ExtendedBinaryReader reader)
        {
            this.reader = reader;
        }

        public void Dispose() => reader.Dispose();

        public object ReadValue(ClickHouseType databaseType)
        {
            switch (databaseType.TypeCode)
            {
                case ClickHouseTypeCode.UInt8:
                    return reader.ReadByte();
                case ClickHouseTypeCode.UInt16:
                    return reader.ReadUInt16();
                case ClickHouseTypeCode.UInt32:
                    return reader.ReadUInt32();
                case ClickHouseTypeCode.UInt64:
                    return reader.ReadUInt64();

                case ClickHouseTypeCode.Int8:
                    return reader.ReadSByte();
                case ClickHouseTypeCode.Int16:
                    return reader.ReadInt16();
                case ClickHouseTypeCode.Int32:
                    return reader.ReadInt32();
                case ClickHouseTypeCode.Int64:
                    return reader.ReadInt64();

                case ClickHouseTypeCode.Float32:
                    return reader.ReadSingle();
                case ClickHouseTypeCode.Float64:
                    return reader.ReadDouble();

                case ClickHouseTypeCode.String:
                    return reader.ReadString();
                case ClickHouseTypeCode.FixedString:
                    var stringInfo = (FixedStringType)databaseType;
                    return Encoding.UTF8.GetString(reader.ReadBytes(stringInfo.Length));

                case ClickHouseTypeCode.Array:
                    var arrayTypeInfo = (ArrayType)databaseType;
                    var length = reader.Read7BitEncodedInt();
                    var data = new object[length];
                    for (var i = 0; i < length; i++)
                        data[i] = ReadValue(arrayTypeInfo.UnderlyingType);
                    return data;

                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)databaseType;
                    return reader.ReadByte() > 0 ? DBNull.Value : ReadValue(nullableTypeInfo.UnderlyingType);

                case ClickHouseTypeCode.Date:
                    var days = reader.ReadUInt16();
                    return TypeConverter.DateTimeEpochStart.AddDays(days);
                case ClickHouseTypeCode.DateTime:
                    var seconds = reader.ReadUInt32();
                    return TypeConverter.DateTimeEpochStart.AddSeconds(seconds);

                case ClickHouseTypeCode.UUID:
                    // Weird byte manipulation because of C#'s strange Guid implementation
                    var bytes = new byte[16];
                    reader.Read(bytes, 6, 2);
                    reader.Read(bytes, 4, 2);
                    reader.Read(bytes, 0, 4);
                    reader.Read(bytes, 8, 8);
                    Array.Reverse(bytes, 8, 8);
                    return new Guid(bytes);

                case ClickHouseTypeCode.Tuple:
                    var tupleTypeInfo = (TupleType)databaseType;
                    var count = tupleTypeInfo.UnderlyingTypes.Length;
                    var contents = new object[count];
                    for (var i = 0; i < count; i++)
                    {
                        contents[i] = ReadValue(tupleTypeInfo.UnderlyingTypes[i]);
                        if (contents[i] is DBNull)
                            contents[i] = null;
                    }
                    return tupleTypeInfo.MakeTuple(contents);

                case ClickHouseTypeCode.Decimal:
                    var decimalTypeInfo = (DecimalType)databaseType;
                    var factor = (int)Math.Pow(10, decimalTypeInfo.Scale);
                    var value = new BigInteger(reader.ReadBytes(decimalTypeInfo.Size));
                    return (decimal)value / factor;
            }
            throw new NotImplementedException();
        }
    }
}
