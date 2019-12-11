using System;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal static class BinaryFormat
    {
        public static object ReadValue(ExtendedBinaryReader reader, ClickHouseType databaseType)
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
                        data[i] = ReadValue(reader, arrayTypeInfo.UnderlyingType);
                    return data;
                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)databaseType;
                    return reader.ReadByte() > 0 ? DBNull.Value : ReadValue(reader, nullableTypeInfo.UnderlyingType);

                case ClickHouseTypeCode.Date:
                    var days = reader.ReadUInt16();
                    return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(days);
                case ClickHouseTypeCode.DateTime:
                    var milliseconds = reader.ReadUInt32();
                    return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(milliseconds);

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
                        contents[i] = ReadValue(reader, tupleTypeInfo.UnderlyingTypes[i]);
                    return contents;

                case ClickHouseTypeCode.Decimal:
                    var decimalTypeInfo = (DecimalType)databaseType;
                    var scale = decimalTypeInfo.Scale;
                    var factor = (int)Math.Pow(10, scale);
                    var value = new BigInteger(reader.ReadBytes(decimalTypeInfo.Size));
                    return (decimal)value / factor;
            }
            throw new NotImplementedException();
        }

        private static void WriteValue(ExtendedBinaryWriter writer, object data, ClickHouseType databaseType)
        {
            switch (databaseType.TypeCode)
            {
                case ClickHouseTypeCode.UInt8:
                    writer.Write((byte)data);
                    break;
                case ClickHouseTypeCode.UInt16:
                    writer.Write((ushort)data);
                    break;
                case ClickHouseTypeCode.UInt32:
                    writer.Write((uint)data);
                    break;
                case ClickHouseTypeCode.UInt64:
                    writer.Write((ulong)data);
                    break;
                case ClickHouseTypeCode.Int8:
                    writer.Write((sbyte)data);
                    break;
                case ClickHouseTypeCode.Int16:
                    writer.Write((short)data);
                    break;
                case ClickHouseTypeCode.Int32:
                    writer.Write((int)data);
                    break;
                case ClickHouseTypeCode.Int64:
                    writer.Write((long)data);
                    break;
                case ClickHouseTypeCode.Float32:
                    writer.Write((float)data);
                    break;
                case ClickHouseTypeCode.Float64:
                    writer.Write((double)data);
                    break;
                case ClickHouseTypeCode.String:
                    writer.Write((string)data);
                    break;
                case ClickHouseTypeCode.FixedString:
                    var stringInfo = (FixedStringType)databaseType;
                    var buffer = Encoding.UTF8.GetBytes((string)data);
                    if (buffer.Length > stringInfo.Length)
                        throw new InvalidOperationException(Resources.StringIsTooLargeForFixedStringMessage);
                    writer.Write(buffer);
                    var delta = stringInfo.Length - buffer.Length;
                    for (var i = 0; i < delta; i++)
                        writer.Write((byte)0); // Add padding to reach the size of FixedString
                    break;
                case ClickHouseTypeCode.Array:
                    var arrayTypeInfo = (ArrayType)databaseType;
                    var array = (object[])data;
                    writer.Write7BitEncodedInt(array.Length);
                    for (var i = 0; i < array.Length; i++)
                        WriteValue(writer, array[i], arrayTypeInfo.UnderlyingType);
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
                        WriteValue(writer, data, nullableTypeInfo.UnderlyingType);
                    }
                    break;
            }
            throw new NotImplementedException();
        }
    }
}
