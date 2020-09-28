using System.IO;
using System.Net.Http;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseBinaryReader : ClickHouseDataReader
    {
        private const int BufferSize = 512 * 1024;

        private readonly ExtendedBinaryReader reader;
        private readonly BinaryStreamReader streamReader;

        public ClickHouseBinaryReader(HttpResponseMessage httpResponse)
            : base(httpResponse)
        {
            var stream = new BufferedStream(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult(), BufferSize);
            reader = new ExtendedBinaryReader(stream); // will dispose of stream
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
            {
                FieldNames[i] = reader.ReadString();
            }

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
                    var rawType = RawTypes[i];
                    data[i] = streamReader.Read(rawType);
                }
                return true;
            }
            catch (EndOfStreamException)
            {
                // HACK this is a horrible hack related to the fact that GZip-compressed stream
                // does not provide a Peek method for some reason, forcing us to only be able to
                // detect EOF by actually trying to read
                return false;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                reader?.Dispose();
                streamReader?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
