using System;
using System.IO;
using System.Text;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class RowBinaryWriter : IRowDataWriter, IDisposable
    {
        private readonly ClickHouseType[] columnTypes;
        private readonly ExtendedBinaryWriter writer;

        public RowBinaryWriter(Stream destination, params ClickHouseType[] columnTypes)
        {
            this.columnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
            writer = new ExtendedBinaryWriter(destination);
        }

        public void WriteRow(params object[] row)
        {
            var count = columnTypes.Length;
            if (row.Length != count)
                throw new ArgumentException(Resources.InvalidNumberOfColumnsInRowMessage, nameof(row));
            for (var i = 0; i < count; i++)
            {
                var type = columnTypes[i];
                WriteItem(row[i], type);
            }
        }

        private void WriteItem(object data, ClickHouseType rawTypeInfo)
        {
            switch (rawTypeInfo.DataType)
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
                    var stringInfo = (FixedStringType)rawTypeInfo;
                    var buffer = Encoding.UTF8.GetBytes((string)data);
                    if (buffer.Length > stringInfo.Length)
                        throw new InvalidOperationException(Resources.StringIsTooLargeForFixedStringMessage);
                    writer.Write(buffer);
                    var delta = stringInfo.Length - buffer.Length;
                    for (var i = 0; i < delta; i++)
                        writer.Write((byte)0); // Add padding to reach the size of FixedString
                    break;
                case ClickHouseTypeCode.Array:
                    var arrayTypeInfo = (ArrayType)rawTypeInfo;
                    var array = (object[])data;
                    writer.Write7BitEncodedInt(array.Length);
                    for (var i = 0; i < array.Length; i++)
                        WriteItem(array[i], arrayTypeInfo.UnderlyingType);
                    break;
                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)rawTypeInfo;
                    if (data == null || data is DBNull)
                    {
                        writer.Write((byte)1);
                    }
                    else
                    {
                        writer.Write((byte)0);
                        WriteItem(data, nullableTypeInfo.UnderlyingType);
                    }
                    break;
            }
            throw new NotImplementedException();
        }

        public void Dispose() => writer?.Dispose();
    }
}
