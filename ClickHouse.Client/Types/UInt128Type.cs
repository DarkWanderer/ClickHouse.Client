namespace ClickHouse.Client.Types;

internal class UInt128Type : AbstractBigIntegerType
{
    public override int Size => 16;

    public override string ToString() => "UInt128";

    public override bool Signed => false;
}
