using System;
using System.Net;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Formats
{
    internal class BinaryStreamReader : IDisposable
    {
        private readonly ExtendedBinaryReader reader;

        public BinaryStreamReader(ExtendedBinaryReader reader)
        {
            this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public void Dispose() => reader.Dispose();

        public object ReadValue(ClickHouseType databaseType, bool nullAsDbNull)
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
                    {
                        data[i] = ReadValue(arrayTypeInfo.UnderlyingType, nullAsDbNull);
                    }

                    return data;

                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)databaseType;
                    if (reader.ReadByte() > 0)
                    {
                        return nullAsDbNull ? DBNull.Value : null;
                    }
                    else
                    {
                        return ReadValue(nullableTypeInfo.UnderlyingType, nullAsDbNull);
                    }

                case ClickHouseTypeCode.Date:
                    var days = reader.ReadUInt16();
                    return TypeConverter.DateTimeEpochStart.AddDays(days);
                case ClickHouseTypeCode.DateTime:
                    var seconds = reader.ReadUInt32();
                    return TypeConverter.DateTimeEpochStart.AddSeconds(seconds);
                case ClickHouseTypeCode.DateTime64:
                    var dt64t = (DateTime64Type)databaseType;
                    var chTicks = reader.ReadInt64();
                    // 7 is a 'magic constant' - Log10 of TimeSpan.TicksInSecond
                    return TypeConverter.DateTimeEpochStart.AddTicks((long)MathUtils.ShiftDecimalPlaces(chTicks, 7 - dt64t.Scale));

                case ClickHouseTypeCode.UUID:
                    // Byte manipulation because of ClickHouse's weird GUID implementation
                    var bytes = new byte[16];
                    reader.Read(bytes, 6, 2);
                    reader.Read(bytes, 4, 2);
                    reader.Read(bytes, 0, 4);
                    reader.Read(bytes, 8, 8);
                    Array.Reverse(bytes, 8, 8);
                    return new Guid(bytes);

                case ClickHouseTypeCode.IPv4:
                    var ipv4bytes = reader.ReadBytes(4);
                    Array.Reverse(ipv4bytes);
                    return new IPAddress(ipv4bytes);

                case ClickHouseTypeCode.IPv6:
                    var ipv6bytes = reader.ReadBytes(16);
                    return new IPAddress(ipv6bytes);

                case ClickHouseTypeCode.Tuple:
                    var tupleTypeInfo = (TupleType)databaseType;
                    var count = tupleTypeInfo.UnderlyingTypes.Length;
                    var contents = new object[count];
                    for (var i = 0; i < count; i++)
                    {
                        // Underlying data in Tuple should always be null, not DBNull
                        contents[i] = ReadValue(tupleTypeInfo.UnderlyingTypes[i], false);
                    }
                    return tupleTypeInfo.MakeTuple(contents);

                case ClickHouseTypeCode.Decimal:
                    var decimalTypeInfo = (DecimalType)databaseType;
                    var value = new BigInteger(reader.ReadBytes(decimalTypeInfo.Size));
                    return MathUtils.ShiftDecimalPlaces((decimal)value, -decimalTypeInfo.Scale);
                case ClickHouseTypeCode.Nothing:
                    break;
                case ClickHouseTypeCode.Nested:
                    throw new NotSupportedException("Nested types cannot be read directly");

                case ClickHouseTypeCode.Enum8:
                    var enum8TypeInfo = (EnumType)databaseType;
                    return enum8TypeInfo.Lookup(reader.ReadSByte());

                case ClickHouseTypeCode.Enum16:
                    var enum16TypeInfo = (EnumType)databaseType;
                    return enum16TypeInfo.Lookup(reader.ReadInt16());

                case ClickHouseTypeCode.LowCardinality:
                    var lcCardinality = (LowCardinalityType)databaseType;
                    return ReadValue(lcCardinality.UnderlyingType, nullAsDbNull);
            }
            throw new NotImplementedException($"Reading of {databaseType.TypeCode} is not implemented");
        }
    }
}
