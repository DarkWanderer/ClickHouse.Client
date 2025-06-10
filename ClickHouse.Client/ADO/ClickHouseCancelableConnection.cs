using System.Data.Common;
using System.Net.Http;

#if NET7_0_OR_GREATER

namespace ClickHouse.Client.ADO;

public class ClickHouseCancelableConnection : ClickHouseConnection
{
    public ClickHouseCancelableConnection()
        : base() { }

    public ClickHouseCancelableConnection(string connectionString)
        : base(connectionString) { }

    public ClickHouseCancelableConnection(string connectionString, HttpClient httpClient)
        : base(connectionString, httpClient) { }

    public ClickHouseCancelableConnection(string connectionString, IHttpClientFactory httpClientFactory, string httpClientName = "")
        : base(connectionString, httpClientFactory, httpClientName) { }

    public new ClickHouseCancelableCommand CreateCommand() => new ClickHouseCancelableCommand(this);

    protected override DbCommand CreateDbCommand() => CreateCommand();
}
#endif
