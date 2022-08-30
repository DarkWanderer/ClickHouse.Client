using System;
using System.IO;
using System.Runtime.InteropServices.ComTypes;
using System.Text;

namespace ClickHouse.Client.Formats
{
    internal class ExtendedBinaryReader : BinaryReader
    {
        private readonly PeekableStreamWrapper streamWrapper;

        public ExtendedBinaryReader(Stream stream)
            : base(new PeekableStreamWrapper(stream), Encoding.UTF8, false)
        {
            streamWrapper = (PeekableStreamWrapper)BaseStream;
        }

        public new int Read7BitEncodedInt() => base.Read7BitEncodedInt();

        public override byte[] ReadBytes(int count)
        {
            var buffer = new byte[count];
            Read(buffer, 0, count);
            return buffer;
        }

        public override int Read(byte[] buffer, int index, int count)
        {
            int read = 0;
            do
            {
                int num2 = base.Read(buffer, index + read, count - read);
                read += num2;
                if (read < count && PeekChar() == -1)
                {
                    throw new EndOfStreamException($"Expected to read {count} bytes, got {read}");
                }
            }
            while (read < count);

            return read;
        }

        public override int PeekChar() => streamWrapper.Peek();
    }
}
