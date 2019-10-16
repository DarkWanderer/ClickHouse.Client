using System;
using System.IO;
using System.Net.Http;
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
            var count = FieldCount;
            var data = new object[count];
            for (int i = 0; i < count; i++)
            {
                switch (RawTypes[i])
                {
                    case DataType.UInt16:
                        break;
                    case DataType.UInt32:
                        break;
                    case DataType.UInt64:
                        break;
                    case DataType.Int16:
                        break;
                    case DataType.Int32:
                        break;
                    case DataType.Int64:
                        data[i] = ReadInt64(stream);
                        break;
                    case DataType.Float32:
                        break;
                    case DataType.Float64:
                        break;
                    case DataType.String:
                        break;
                    case DataType.DateTime:
                        break;
                    case DataType.UInt8:
                        data[i] = ReadUInt8(stream);
                        break;
                    case DataType.Int8:
                        break;
                    case DataType.Date:
                        break;
                    case DataType.FixedString:
                        break;
                }
            }
            CurrentRow = data;
            return true;
        }

        private long ReadInt64(Stream stream)
        {
            var length = sizeof(Int64);
            var bytes = new byte[length];
            stream.Read(bytes, 0, length);
            return BitConverter.ToInt64(bytes, 0);
        }
        private byte ReadUInt8(Stream stream) => (byte)stream.ReadByte();
    }
}