using System;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class BinaryStreamWriter : IStreamDataWriter, IDisposable
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
                        WriteValue(array[i], arrayTypeInfo.UnderlyingType);
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
            }
            throw new NotImplementedException();
        }
    }
}
