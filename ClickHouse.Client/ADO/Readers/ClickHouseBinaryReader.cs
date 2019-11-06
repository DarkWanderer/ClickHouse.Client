using System;
using System.IO;
using System.Net.Http;
using System.Numerics;
using System.Text;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseBinaryReader : ClickHouseDataReader
    {
        private readonly Stream stream;
        private readonly ExtendedBinaryReader reader;

        public ClickHouseBinaryReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            stream = httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
            reader = new ExtendedBinaryReader(stream);
            ReadHeaders();
        }

        private void ReadHeaders()
        {
            var count = reader.Read7BitEncodedInt();
            FieldNames = new string[count];
            RawTypes = new ClickHouseTypeInfo[count];

            for (var i = 0; i < count; i++)
                FieldNames[i] = ReadStringBinary(reader);
            for (var i = 0; i < count; i++)
            {
                var chType = ReadStringBinary(reader);
                RawTypes[i] = TypeConverter.ParseClickHouseType(chType);
            }
        }

        private static string ReadStringBinary(ExtendedBinaryReader reader)
        {
            var length = reader.Read7BitEncodedInt();
            return ReadFixedStringBinary(reader, length);
        }

        private static string ReadFixedStringBinary(BinaryReader reader, int length)
        {
            var bytes = reader.ReadBytes(length);
            return Encoding.UTF8.GetString(bytes);
        }

        private bool StreamHasMoreData => stream.Position < stream.Length;

        public override bool HasRows => StreamHasMoreData;

        public override bool Read()
        {
            if (!StreamHasMoreData)
                return false;

            var initialPosition = stream.Position;
            var count = FieldCount;
            var data = new object[count];
            for (var i = 0; i < count; i++)
            {
                var rawTypeInfo = RawTypes[i];
                data[i] = ReadBinaryDataType(reader, rawTypeInfo);
            }
            CurrentRow = data;
            // infinite cycle prevention: if stream position did not move, something went wrong
            if (initialPosition == stream.Position)
                throw new InvalidOperationException(Resources.InternalErrorMessage);
            return true;
        }

        private static object ReadBinaryDataType(ExtendedBinaryReader reader, ClickHouseTypeInfo rawTypeInfo)
        {
            switch (rawTypeInfo.DataType)
            {
                case ClickHouseDataType.UInt8:
                    return reader.ReadByte();
                case ClickHouseDataType.UInt16:
                    return reader.ReadUInt16();
                case ClickHouseDataType.UInt32:
                    return reader.ReadUInt32();
                case ClickHouseDataType.UInt64:
                    return reader.ReadUInt64();

                case ClickHouseDataType.Int8:
                    return reader.ReadSByte();
                case ClickHouseDataType.Int16:
                    return reader.ReadInt16();
                case ClickHouseDataType.Int32:
                    return reader.ReadInt32();
                case ClickHouseDataType.Int64:
                    return reader.ReadInt64();

                case ClickHouseDataType.Float32:
                    return reader.ReadSingle();
                case ClickHouseDataType.Float64:
                    return reader.ReadDouble();
                case ClickHouseDataType.String:
                    return reader.ReadString();
                case ClickHouseDataType.FixedString:
                    var stringInfo = (FixedStringTypeInfo)rawTypeInfo;
                    return ReadFixedStringBinary(reader, stringInfo.Length);

                case ClickHouseDataType.Array:
                    var arrayTypeInfo = (ArrayTypeInfo)rawTypeInfo;
                    var length = reader.Read7BitEncodedInt();
                    var data = new object[length];
                    for (var i = 0; i < length; i++)
                        data[i] = ReadBinaryDataType(reader, arrayTypeInfo.UnderlyingType);
                    return data;
                case ClickHouseDataType.Nullable:
                    var nullableTypeInfo = (NullableTypeInfo)rawTypeInfo;
                    return reader.ReadByte() > 0 ? DBNull.Value : ReadBinaryDataType(reader, nullableTypeInfo.UnderlyingType);

                case ClickHouseDataType.Date:
                    var days = reader.ReadUInt16();
                    return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(days);
                case ClickHouseDataType.DateTime:
                    var milliseconds = reader.ReadUInt32();
                    return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(milliseconds);

                case ClickHouseDataType.UUID:
                    // Weird byte manipulation because of C#'s strange Guid implementation
                    var bytes = new byte[16];
                    reader.Read(bytes, 6, 2);
                    reader.Read(bytes, 4, 2);
                    reader.Read(bytes, 0, 4);
                    reader.Read(bytes, 8, 8);
                    Array.Reverse(bytes, 8, 8);
                    return new Guid(bytes);

                case ClickHouseDataType.Tuple:
                    var tupleTypeInfo = (TupleTypeInfo)rawTypeInfo;
                    var count = tupleTypeInfo.UnderlyingTypes.Length;
                    var contents = new object[count];
                    for (var i = 0; i < count; i++)
                        contents[i] = ReadBinaryDataType(reader, tupleTypeInfo.UnderlyingTypes[i]);
                    return contents;

                case ClickHouseDataType.Decimal:
                    var decimalTypeInfo = (DecimalTypeInfo)rawTypeInfo;
                    var scale = decimalTypeInfo.Scale;
                    var factor = (int)Math.Pow(10, scale);
                    var value = new BigInteger(reader.ReadBytes(decimalTypeInfo.Size));
                    return (decimal)value / factor;
            }
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                reader.Dispose();
                stream.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}