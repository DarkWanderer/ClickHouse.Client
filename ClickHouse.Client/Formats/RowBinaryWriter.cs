using System;
using System.IO;
using System.Text;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    internal class RowBinaryWriter : IRowDataWriter
    {
        private readonly TypeInfo[] columnTypes;
        private readonly ExtendedBinaryWriter writer;

        public RowBinaryWriter(Stream destination, params TypeInfo[] columnTypes)
        {
            this.columnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
            writer = new ExtendedBinaryWriter(destination);
        }

        public void WriteRow(params object[] row)
        {
            var count = columnTypes.Length;
            if (row.Length != count)
                throw new ArgumentException("Invalid number of items in row", nameof(row));
            for (var i = 0; i < count; i++)
            {
                var type = columnTypes[i];
                WriteItem(row[i], type);
            }
        }

        private void WriteItem(object data, TypeInfo rawTypeInfo)
        {
            switch (rawTypeInfo.DataType)
            {
                case ClickHouseDataType.UInt8:
                    writer.Write((byte)data);
                    break;
                case ClickHouseDataType.UInt16:
                    writer.Write((ushort)data);
                    break;
                case ClickHouseDataType.UInt32:
                    writer.Write((uint)data);
                    break;
                case ClickHouseDataType.UInt64:
                    writer.Write((ulong)data);
                    break;
                case ClickHouseDataType.Int8:
                    writer.Write((sbyte)data);
                    break;
                case ClickHouseDataType.Int16:
                    writer.Write((short)data);
                    break;
                case ClickHouseDataType.Int32:
                    writer.Write((int)data);
                    break;
                case ClickHouseDataType.Int64:
                    writer.Write((long)data);
                    break;
                case ClickHouseDataType.Float32:
                    writer.Write((float)data);
                    break;
                case ClickHouseDataType.Float64:
                    writer.Write((double)data);
                    break;
                case ClickHouseDataType.String:
                    writer.Write((string)data);
                    break;
                case ClickHouseDataType.FixedString:
                    var stringInfo = (FixedStringTypeInfo)rawTypeInfo;
                    var buffer = Encoding.UTF8.GetBytes((string)data);
                    if (buffer.Length > stringInfo.Length)
                        throw new InvalidOperationException("String is too large to fit in FixedString");
                    writer.Write(buffer);
                    var delta = stringInfo.Length - buffer.Length;
                    for (var i = 0; i < delta; i++)
                        writer.Write((byte)0); // Add padding to reach the size of FixedString
                    break;
                case ClickHouseDataType.Array:
                    var arrayTypeInfo = (ArrayTypeInfo)rawTypeInfo;
                    var array = (object[])data;
                    writer.Write7BitEncodedInt(array.Length);
                    for (var i = 0; i < array.Length; i++)
                        WriteItem(array[i], arrayTypeInfo.UnderlyingType);
                    break;
                case ClickHouseDataType.Nullable:
                    var nullableTypeInfo = (NullableTypeInfo)rawTypeInfo;
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

    }
}
