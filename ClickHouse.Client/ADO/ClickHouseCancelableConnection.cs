using System.Data.Common;
using System.Net.Http;

namespace ClickHouse.Client.ADO;

#if NET5_0_OR_GREATER
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
