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
            RawTypes = new ClickHouseType[count];
            CurrentRow = new object[count];

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
            var count = RawTypes.Length;
            var data = CurrentRow;
            for (var i = 0; i < count; i++)
            {
                var rawTypeInfo = RawTypes[i];
                data[i] = ReadBinaryDataType(reader, rawTypeInfo);
            }
            // infinite cycle prevention: if stream position did not move, something went wrong
            if (initialPosition == stream.Position)
                throw new InvalidOperationException(Resources.InternalErrorMessage);
            return true;
        }

        private static object ReadBinaryDataType(ExtendedBinaryReader reader, ClickHouseType rawTypeInfo)
        {
            switch (rawTypeInfo.DataType)
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
                    var stringInfo = (FixedStringType)rawTypeInfo;
                    return ReadFixedStringBinary(reader, stringInfo.Length);

                case ClickHouseTypeCode.Array:
                    var arrayTypeInfo = (ArrayType)rawTypeInfo;
                    var length = reader.Read7BitEncodedInt();
                    var data = new object[length];
                    for (var i = 0; i < length; i++)
                        data[i] = ReadBinaryDataType(reader, arrayTypeInfo.UnderlyingType);
                    return data;
                case ClickHouseTypeCode.Nullable:
                    var nullableTypeInfo = (NullableType)rawTypeInfo;
                    return reader.ReadByte() > 0 ? DBNull.Value : ReadBinaryDataType(reader, nullableTypeInfo.UnderlyingType);

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
                    var tupleTypeInfo = (TupleType)rawTypeInfo;
                    var count = tupleTypeInfo.UnderlyingTypes.Length;
                    var contents = new object[count];
                    for (var i = 0; i < count; i++)
                        contents[i] = ReadBinaryDataType(reader, tupleTypeInfo.UnderlyingTypes[i]);
                    return contents;

                case ClickHouseTypeCode.Decimal:
                    var decimalTypeInfo = (DecimalType)rawTypeInfo;
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