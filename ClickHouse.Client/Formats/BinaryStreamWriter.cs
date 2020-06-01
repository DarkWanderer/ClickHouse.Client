using System;
using System.Collections;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Formats
{
    internal class BinaryStreamWriter : IDisposable
    {
        private readonly ExtendedBinaryWriter writer;

        public BinaryStreamWriter(ExtendedBinaryWriter writer)
        {
            this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public void Dispose() => writer.Dispose();

        public void WriteValue(object data, ClickHouseType databaseType)
        {
            switch (databaseType.TypeCode)
            {
                case ClickHouseTypeCode.UInt8:
                    writer.Write(Convert.ToByte(data));
                    break;
                case ClickHouseTypeCode.UInt16:
                    writer.Write(Convert.ToUInt16(data));
                    break;
                case ClickHouseTypeCode.UInt32:
                    writer.Write(Convert.ToUInt32(data));
                    break;
                case ClickHouseTypeCode.UInt64:
                    writer.Write(Convert.ToUInt64(data));
                    break;

                case ClickHouseTypeCode.Int8:
                    writer.Write(Convert.ToSByte(data));
                    break;
                case ClickHouseTypeCode.Int16:
                    writer.Write(Convert.ToInt16(data));
                    break;
                case ClickHouseTypeCode.Int32:
                    writer.Write(Convert.ToInt32(data));
                    break;
                case ClickHouseTypeCode.Int64:
                    writer.Write(Convert.ToInt64(data));
                    break;

                case ClickHouseTypeCode.Float32:
                    writer.Write(Convert.ToSingle(data));
                    break;
                case ClickHouseTypeCode.Float64:
                    writer.Write(Convert.ToDouble(data));
                    break;

                case ClickHouseTypeCode.Decimal:
                    var dti = (DecimalType)databaseType;
                    decimal multipliedValue = Convert.ToDecimal(data) * dti.Exponent;
                    switch (dti.Size)
                    {
                        case 4:
                            writer.Write((int)multipliedValue);
                            break;
                        case 8:
                            writer.Write((long)multipliedValue);
                            break;
                        default:
                            WriteLargeDecimal(dti, multipliedValue);
                            break;
                    }
                    break;

                case ClickHouseTypeCode.String:
                    writer.Write(Convert.ToString(data));
                    break;

                case ClickHouseTypeCode.FixedString:
                    var @string = (string)data;
                    var stringInfo = (FixedStringType)databaseType;
                    var stringBytes = new byte[stringInfo.Length];
                    Encoding.UTF8.GetBytes(@string, 0, @string.Length, stringBytes, 0);

                    writer.Write(stringBytes);
                    break;

                case ClickHouseTypeCode.Array:
                    var arrayTypeInfo = (ArrayType)databaseType;
                    var collection = (IList)data;
                    writer.Write7BitEncodedInt(collection.Count);
                    for (var i = 0; i < collection.Count; i++)
                    {
                        WriteValue(collection[i], arrayTypeInfo.UnderlyingType);
                    }

                    break;
                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)databaseType;
                    if (data == null || data is DBNull)
                    {
                        writer.Write((byte)1);
                    }
                    else
                    {
                        writer.Write((byte)0);
                        WriteValue(data, nullableTypeInfo.UnderlyingType);
                    }
                    break;
                case ClickHouseTypeCode.Tuple:
                    var tupleType = (TupleType)databaseType;
                    var tuple = (ITuple)data;
                    for (var i = 0; i < tuple.Length; i++)
                    {
                        WriteValue(tuple[i], tupleType.UnderlyingTypes[i]);
                    }

                    break;
                case ClickHouseTypeCode.Nested:
                    var nestedType = (NestedType)databaseType;
                    var tuples = ((IEnumerable)data).Cast<ITuple>().ToList();
                    writer.Write7BitEncodedInt(tuples.Count);
                    foreach (var ntuple in tuples)
                    {
                        for (int i = 0; i < ntuple.Length; i++)
                        {
                            WriteValue(ntuple[i], nestedType.UnderlyingTypes[i]);
                        }
                    }

                    break;

                case ClickHouseTypeCode.UUID:
                    var guid = ExtractGuid(data);
                    var bytes = guid.ToByteArray();
                    Array.Reverse(bytes, 8, 8);
                    writer.Write(bytes, 6, 2);
                    writer.Write(bytes, 4, 2);
                    writer.Write(bytes, 0, 4);
                    writer.Write(bytes, 8, 8);
                    break;

                case ClickHouseTypeCode.IPv4:
                    var address4 = ExtractIPAddress(data);
                    if (address4.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
                    {
                        throw new ArgumentException($"Expected IPv4, got {address4}");
                    }

                    var ipv4bytes = address4.GetAddressBytes();
                    Array.Reverse(ipv4bytes);
                    writer.Write(ipv4bytes, 0, ipv4bytes.Length);
                    break;

                case ClickHouseTypeCode.IPv6:
                    var address6 = ExtractIPAddress(data);
                    if (address6.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
                    {
                        throw new ArgumentException($"Expected IPv4, got {address6}");
                    }

                    var ipv6bytes = address6.GetAddressBytes();
                    writer.Write(ipv6bytes, 0, ipv6bytes.Length);
                    break;

                case ClickHouseTypeCode.Date:
                    var days = (ushort)(((DateTime)data).Date - TypeConverter.DateTimeEpochStart).TotalDays;
                    writer.Write(days);
                    break;
                case ClickHouseTypeCode.DateTime:
                    var dtType = (AbstractDateTimeType)databaseType;
                    var dto = dtType.ToDateTimeOffset((DateTime)data);
                    var seconds = (uint)(dto.UtcDateTime - TypeConverter.DateTimeEpochStart).TotalSeconds;
                    writer.Write(seconds);
                    break;
                case ClickHouseTypeCode.DateTime64:
                    var dt64type = (DateTime64Type)databaseType;
                    var dto64 = dt64type.ToDateTimeOffset((DateTime)data);
                    var ticks = (dto64.UtcDateTime - TypeConverter.DateTimeEpochStart).Ticks;
                    // 7 is a 'magic constant' - Log10 of TimeSpan.TicksInSecond
                    writer.Write(MathUtils.ShiftDecimalPlaces(ticks, dt64type.Scale - 7));
                    break;

                case ClickHouseTypeCode.Nothing:
                    break;

                case ClickHouseTypeCode.Enum8:
                    var enum8TypeInfo = (EnumType)databaseType;
                    var enum8Index = data is string enum8Str ? (sbyte)enum8TypeInfo.Lookup(enum8Str) : Convert.ToSByte(data);
                    writer.Write(enum8Index);
                    break;
                case ClickHouseTypeCode.Enum16:
                    var enum16TypeInfo = (EnumType)databaseType;
                    var enum16Index = data is string enum16Str ? (short)enum16TypeInfo.Lookup(enum16Str) : Convert.ToInt16(data);
                    writer.Write(enum16Index);
                    break;

                case ClickHouseTypeCode.LowCardinality:
                    var lcCardinality = (LowCardinalityType)databaseType;
                    WriteValue(data, lcCardinality.UnderlyingType);
                    break;

                default:
                    throw new NotImplementedException($"{databaseType.TypeCode} not supported yet");
            }
        }

        private void WriteLargeDecimal(DecimalType dti, decimal value)
        {
            var bigInt = new BigInteger(value);
            byte[] bigIntBytes = bigInt.ToByteArray();
            byte[] decimalBytes = new byte[dti.Size];
            bigIntBytes.CopyTo(decimalBytes, 0);

            // If a negative BigInteger is not long enough to fill the whole buffer, the remainder needs to be filled with 0xFF
            if (bigInt < 0)
            {
                for (int i = bigIntBytes.Length; i < dti.Size; i++)
                    decimalBytes[i] = 0xFF;
            }
            writer.Write(decimalBytes);
        }

        private static Guid ExtractGuid(object data)
        {
            if (data is Guid g)
            {
                return g;
            }
            else if (data is string s)
            {
                return new Guid(s);
            }
            else
            {
                throw new ArgumentException($"Cannot convert {data?.GetType()?.Name ?? "null"} to GUID");
            }
        }

        private static IPAddress ExtractIPAddress(object data)
        {
            if (data is IPAddress a)
            {
                return a;
            }
            else if (data is string s)
            {
                return IPAddress.Parse(s);
            }
            else
            {
                throw new ArgumentException($"Cannot convert {data?.GetType()?.Name ?? "null"} to IP address");
            }
        }
    }
}
