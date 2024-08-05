namespace ClickHouse.Client.ADO;

#pragma warning disable SA1313 // Parameter names should begin with lower-case letter
public record class QueryStats(long ReadRows, long ReadBytes, long WrittenRows, long WrittenBytes, long TotalRowsToRead);
#pragma warning restore SA1313 // Parameter names should begin with lower-case letter
