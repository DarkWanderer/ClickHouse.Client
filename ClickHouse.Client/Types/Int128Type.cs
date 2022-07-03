namespace ClickHouse.Client.Types
{
    internal class Int128Type : AbstractBigIntegerType
    {
        public override int Size => 16;

        public override string ToString() => "Int128";
    }
}
