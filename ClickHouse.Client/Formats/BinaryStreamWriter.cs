using System;
using System.Collections;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class BinaryStreamWriter : IDisposable
    {
        private readonly ExtendedBinaryWriter writer;

        public BinaryStreamWriter(ExtendedBinaryWriter writer)
        {
            this.writer = writer;
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
                    var factor = (int)Math.Pow(10, dti.Scale);
                    var value = new BigInteger(Convert.ToDecimal(data) * factor);
                    var dbytes = new byte[dti.Size];
                    value.ToByteArray().CopyTo(dbytes, 0);
                    writer.Write(dbytes);
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
                        WriteValue(collection[i], arrayTypeInfo.UnderlyingType);
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
                    var tupleTypeInfo = (TupleType)databaseType;
                    var tuple = (ITuple)data;
                    for (var i = 0; i < tuple.Length; i++)
                        WriteValue(tuple[i], tupleTypeInfo.UnderlyingTypes[i]);
                    break;

                case ClickHouseTypeCode.UUID:
                    Guid guid;
                    if (data is Guid g)
                        guid = g;
                    else if (data is string s)
                        guid = new Guid(s);
                    else
                        throw new NotSupportedException($"Cannot convert {data?.GetType()?.Name ?? "null"} to GUID");

                    var bytes = guid.ToByteArray();
                    Array.Reverse(bytes, 8, 8);
                    writer.Write(bytes, 6, 2);
                    writer.Write(bytes, 4, 2);
                    writer.Write(bytes, 0, 4);
                    writer.Write(bytes, 8, 8);
                    break;
                case ClickHouseTypeCode.DateTime:
                    var seconds = (uint)((DateTime)data - TypeConverter.DateTimeEpochStart).TotalSeconds;
                    writer.Write(seconds);
                    break;
                case ClickHouseTypeCode.Date:
                    var days = (ushort)((DateTime)data - TypeConverter.DateTimeEpochStart).TotalDays;
                    writer.Write(days);
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
                    var enum16Index = data is string enum16Str ? (sbyte)enum16TypeInfo.Lookup(enum16Str) : Convert.ToInt16(data);
                    writer.Write(enum16Index);
                    break;

                case ClickHouseTypeCode.LowCardinality:
                    var lcCardinality = (LowCardinalityType)databaseType;
                    WriteValue(data, lcCardinality.UnderlyingType);
                    break;

                default:
                    throw new NotImplementedException($"Saving of {databaseType.TypeCode} is not implemented");
            }
        }
    }
}
