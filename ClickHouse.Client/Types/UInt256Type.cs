using System.Numerics;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class UInt256Type : AbstractBigIntegerType
{
    public override int Size => 32;

    public override string ToString() => "UInt256";

    public override bool Signed => false;
}
