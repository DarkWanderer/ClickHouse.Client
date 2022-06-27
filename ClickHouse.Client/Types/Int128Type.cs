using System;
using System.Numerics;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class Int128Type : AbstractBigIntegerType
    {
        public override int Size => 16;

        public override string ToString() => "Int128";
    }
}
