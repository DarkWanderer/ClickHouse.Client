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
        private DataType[] RawTypes;

        public ClickHouseBinaryReader(HttpResponseMessage httpResponse) : base(httpResponse) 
        {
            stream = httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            ReadHeaders();
        }

        private void ReadHeaders()
        {
            var count = ReadLEB128Integer(stream);
            FieldNames = new string[count];
            FieldTypes = new Type[count];
            RawTypes = new DataType[count];

            for (int i = 0; i < count; i++)
                FieldNames[i] = ReadStringBinary(stream);
            for (int i = 0; i < count; i++)
            {
                var chType = ReadStringBinary(stream);
                FieldTypes[i] = TypeConverter.FromClickHouseType(chType);
                RawTypes[i] = TypeConverter.GetClickHouseSimpleType(chType);
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
                switch (RawTypes[i])
                {
                    case DataType.UInt16:
                        data[i] = BitConverter.ToUInt16(ReadBytesForType<ushort>(stream), 0);
                        break;
                    case DataType.UInt32:
                        data[i] = BitConverter.ToUInt32(ReadBytesForType<uint>(stream), 0);
                        break;
                    case DataType.UInt64:
                        data[i] = BitConverter.ToUInt64(ReadBytesForType<ulong>(stream), 0);
                        break;
                    case DataType.Int16:
                        data[i] = BitConverter.ToInt16(ReadBytesForType<short>(stream), 0);
                        break;
                    case DataType.Int32:
                        data[i] = BitConverter.ToInt32(ReadBytesForType<int>(stream), 0);
                        break;
                    case DataType.Int64:
                        data[i] = BitConverter.ToInt64(ReadBytesForType<long>(stream), 0);
                        break;
                    case DataType.Float32:
                        data[i] = BitConverter.ToSingle(ReadBytesForType<float>(stream), 0);
                        break;
                    case DataType.Float64:
                        data[i] = BitConverter.ToDouble(ReadBytesForType<double>(stream), 0);
                        break;
                    case DataType.String:
                        data[i] = ReadStringBinary(stream);
                        break;
                    case DataType.DateTime:
                        break;
                    case DataType.UInt8:
                        data[i] = (byte)stream.ReadByte();
                        break;
                    case DataType.Int8:
                        data[i] = (sbyte)stream.ReadByte();
                        break;
                    case DataType.Date:
                        throw new NotImplementedException();
                    case DataType.FixedString:
                        throw new NotImplementedException();
                }
            }
            CurrentRow = data;
            // infinite cycle prevention: if stream position did not move, something went wrong
            if (initialPosition == stream.Position)
                throw new InvalidOperationException("Internal error: stale stream");
            return true;
        }

        private byte[] ReadBytesForType<T>(Stream stream) where T: struct
        {
            var length = Marshal.SizeOf<T>();
            var bytes = new byte[length];
            stream.Read(bytes, 0, length);
            return bytes;
        }

    }
}