using System.Data.Common;
using System.Net.Http;

#if NET7_0_OR_GREATER

namespace ClickHouse.Client.ADO;

public class ClickHouseCancellableConnection : ClickHouseConnection
{
    public ClickHouseCancellableConnection()
        : base() { }

    public ClickHouseCancellableConnection(string connectionString)
        : base(connectionString) { }

    public ClickHouseCancellableConnection(string connectionString, HttpClient httpClient)
        : base(connectionString, httpClient) { }

    public ClickHouseCancellableConnection(string connectionString, IHttpClientFactory httpClientFactory, string httpClientName = "")
        : base(connectionString, httpClientFactory, httpClientName) { }

    public new ClickHouseCancellableCommand CreateCommand() => new ClickHouseCancellableCommand(this);

    protected override DbCommand CreateDbCommand() => CreateCommand();
}
#endif
