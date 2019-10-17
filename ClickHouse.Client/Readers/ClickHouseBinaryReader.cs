using System;
using System.IO;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using ClickHouse.Client.Types;

namespace ClickHouse.Client
{
    // TODO: use Span to avoid creating byte arrays all the time
    internal class ClickHouseBinaryReader : ClickHouseDataReader
    {
        private readonly Stream stream;

        public ClickHouseBinaryReader(HttpResponseMessage httpResponse) : base(httpResponse) 
        {
            stream = httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            ReadHeaders();
        }

        private void ReadHeaders()
        {
            var count = ReadLEB128Integer(stream);
            FieldNames = new string[count];
            RawTypes = new TypeInfo[count];

            for (int i = 0; i < count; i++)
                FieldNames[i] = ReadStringBinary(stream);
            for (int i = 0; i < count; i++)
            {
                var chType = ReadStringBinary(stream);
                RawTypes[i] = TypeConverter.ParseClickHouseType(chType);
            }
        }

        private static int ReadLEB128Integer(Stream stream)
        {
            int result = 0;
            byte shift = 0;
            while (true)
            {
                int @byte = stream.ReadByte();
                if (@byte < 0)
                    throw new InvalidOperationException();
                result |= (@byte & 0x7F) << shift;
                if ((@byte & 0x80) == 0)
                    break;
                shift += 7;
            }
            return result;
        }

        private static string ReadStringBinary(Stream stream)
        {
            var length = ReadLEB128Integer(stream);
            return ReadFixedStringBinary(stream, length);
        }
        private static string ReadFixedStringBinary(Stream stream, int length)
        {
            var bytes = new byte[length];
            stream.Read(bytes, 0, length);
            return Encoding.UTF8.GetString(bytes);
        }

        public override bool HasRows => stream.Position < stream.Length;

        public override bool Read()
        {
            if (stream.Position >= stream.Length)
                return false;

            var initialPosition = stream.Position;
            var count = FieldCount;
            var data = new object[count];
            for (int i = 0; i < count; i++)
            {
                var rawTypeInfo = RawTypes[i];
                data[i] = ReadBinaryDataType(stream, rawTypeInfo);
            }
            CurrentRow = data;
            // infinite cycle prevention: if stream position did not move, something went wrong
            if (initialPosition == stream.Position)
                throw new InvalidOperationException("Internal error: stale stream");
            return true;
        }

        private static object ReadBinaryDataType(Stream stream, TypeInfo rawTypeInfo)
        {
            switch (rawTypeInfo.DataType)
            {
                case ClickHouseDataType.UInt8:
                    return (byte)stream.ReadByte();
                case ClickHouseDataType.UInt16:
                    return BitConverter.ToUInt16(ReadBytesForType<ushort>(stream), 0);
                case ClickHouseDataType.UInt32:
                    return BitConverter.ToUInt32(ReadBytesForType<uint>(stream), 0);
                case ClickHouseDataType.UInt64:
                    return BitConverter.ToUInt64(ReadBytesForType<ulong>(stream), 0);

                case ClickHouseDataType.Int8:
                    return (sbyte)stream.ReadByte();
                case ClickHouseDataType.Int16:
                    return BitConverter.ToInt16(ReadBytesForType<short>(stream), 0);
                case ClickHouseDataType.Int32:
                    return BitConverter.ToInt32(ReadBytesForType<int>(stream), 0);
                case ClickHouseDataType.Int64:
                    return BitConverter.ToInt64(ReadBytesForType<long>(stream), 0);

                case ClickHouseDataType.Float32:
                    return BitConverter.ToSingle(ReadBytesForType<float>(stream), 0);
                case ClickHouseDataType.Float64:
                    return BitConverter.ToDouble(ReadBytesForType<double>(stream), 0);

                case ClickHouseDataType.String:
                    return ReadStringBinary(stream);
                case ClickHouseDataType.FixedString:
                    var stringInfo = (FixedStringTypeInfo)rawTypeInfo;
                    return ReadFixedStringBinary(stream, stringInfo.Length);

                case ClickHouseDataType.Array:
                    var arrayTypeInfo = (ArrayTypeInfo)rawTypeInfo;
                    var length = ReadLEB128Integer(stream);
                    var data = new object[length];
                    for (int i = 0; i < length; i++)
                        data[i] = ReadBinaryDataType(stream, arrayTypeInfo.UnderlyingType);
                    return data;
                case ClickHouseDataType.Nullable:
                    var nullableTypeInfo = (NullableTypeInfo)rawTypeInfo;
                    return (byte)stream.ReadByte() > 0 ? DBNull.Value : ReadBinaryDataType(stream, nullableTypeInfo.UnderlyingType);
            }
            throw new NotImplementedException();
        }

        private static byte[] ReadBytesForType<T>(Stream stream) where T: struct
        {
            var length = Marshal.SizeOf<T>();
            var bytes = new byte[length];
            stream.Read(bytes, 0, length);
            return bytes;
        }

    }
}