using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ClickHouse.Client
{
    internal class ExtendedBinaryReader : BinaryReader
    {
        public ExtendedBinaryReader(Stream stream) : base(stream) { }

        public new int Read7BitEncodedInt() => base.Read7BitEncodedInt();
    }
}
