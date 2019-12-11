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
                data[i] = BinaryFormat.ReadValue(reader, rawTypeInfo);
            }
            // infinite cycle prevention: if stream position did not move, something went wrong
            if (initialPosition == stream.Position)
                throw new InvalidOperationException(Resources.InternalErrorMessage);
            return true;
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