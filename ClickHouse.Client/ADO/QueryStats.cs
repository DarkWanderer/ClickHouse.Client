namespace ClickHouse.Client.ADO;

public record class QueryStats(int ReadRows, int ReadBytes, int WrittenRows, int WrittenBytes, int TotalRowsToRead);
