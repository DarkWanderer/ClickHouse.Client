namespace ClickHouse.Client.ADO;

#pragma warning disable SA1313 // Parameter names should begin with lower-case letter

public record QueryStats(
    long ReadRows,
    long ReadBytes,
    long WrittenRows,
    long WrittenBytes,
    long TotalRowsToRead,
    long? ResultRows,
    long? ResultBytes,
    long ElapsedNs)
{
    internal QueryStats WithResponseBufferingDisabled()
    {
        return this with { ResultRows = null, ResultBytes = null };
    }
}

#pragma warning restore SA1313 // Parameter names should begin with lower-case letter
