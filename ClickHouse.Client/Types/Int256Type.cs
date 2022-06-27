﻿using System;
using System.Numerics;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{

    internal class Int256Type : AbstractBigIntegerType
    {
        public override int Size => 32;

        public override string ToString() => "Int256";
    }
}
