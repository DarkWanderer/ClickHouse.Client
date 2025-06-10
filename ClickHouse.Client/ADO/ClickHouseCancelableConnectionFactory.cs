using System.Data.Common;
using ClickHouse.Client.ADO.Adapters;
using ClickHouse.Client.ADO.Parameters;

namespace ClickHouse.Client.ADO;

#if NET7_0_OR_GREATER
public class ClickHouseCancelableConnectionFactory : DbProviderFactory
{
    public static ClickHouseCancelableConnectionFactory Instance => new();

    public override DbConnection CreateConnection() => new ClickHouseCancelableConnection();

    public override DbDataAdapter CreateDataAdapter() => new ClickHouseDataAdapter();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new ClickHouseConnectionStringBuilder();

    public override DbParameter CreateParameter() => new ClickHouseDbParameter();

    public override DbCommand CreateCommand() => new ClickHouseCancelableCommand();

    public override DbDataSource CreateDataSource(string connectionString) => new ClickHouseDataSource(connectionString);
}
#endif
