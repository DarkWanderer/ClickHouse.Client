namespace ClickHouse.Client.ADO;

#pragma warning disable SA1313 // Parameter names should begin with lower-case letter
public record class QueryStats(int ReadRows, int ReadBytes, int WrittenRows, int WrittenBytes, int TotalRowsToRead);
#pragma warning restore SA1313 // Parameter names should begin with lower-case letter
