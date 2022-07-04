namespace ClickHouse.Client.Types
{
    internal abstract class IntegerType : ClickHouseType
    {
        public virtual bool Signed => true;
    }
}
