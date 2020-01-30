using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseBinaryReader : ClickHouseDataReader
    {
        private const int bufferSize = 512 * 1024;

        private readonly Stream stream;
        private readonly ExtendedBinaryReader reader;
        private readonly BinaryStreamReader streamReader;

        public ClickHouseBinaryReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            stream = new BufferedStream(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult(), bufferSize);
            reader = new ExtendedBinaryReader(stream);
            streamReader = new BinaryStreamReader(reader);
            ReadHeaders();
        }

        private void ReadHeaders()
        {
            var count = reader.Read7BitEncodedInt();
            FieldNames = new string[count];
            RawTypes = new ClickHouseType[count];
            CurrentRow = new object[count];

            for (var i = 0; i < count; i++)
                FieldNames[i] = reader.ReadString();
            for (var i = 0; i < count; i++)
            {
                var chType = reader.ReadString();
                RawTypes[i] = TypeConverter.ParseClickHouseType(chType);
            }
        }

        public override bool Read()
        {
            try
            {
                var count = RawTypes.Length;
                var data = CurrentRow;
                for (var i = 0; i < count; i++)
                {
                    var rawTypeInfo = RawTypes[i];
                    data[i] = streamReader.ReadValue(rawTypeInfo);
                }
                return true;
            } 
            catch (EndOfStreamException)
            {
                return false;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                reader?.Dispose();
                stream?.Dispose();
                streamReader?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}