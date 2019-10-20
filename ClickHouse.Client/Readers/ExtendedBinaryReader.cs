using System.IO;

namespace ClickHouse.Client
{
    internal class ExtendedBinaryReader : BinaryReader
    {
        public ExtendedBinaryReader(Stream stream) : base(stream) { }

        public new int Read7BitEncodedInt() => base.Read7BitEncodedInt();
    }
}
