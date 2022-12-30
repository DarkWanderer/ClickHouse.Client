using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ClickHouse.Client.Live;

/// <summary>
/// Provides experimental support for WATCH LIVE VIEW feature
/// See https://clickhouse.com/docs/en/sql-reference/statements/watch/
/// WARNING: API may change in future
/// </summary>
public class LiveViewWatcher : IDisposable
{
    private readonly CancellationTokenSource cts = new();
    private readonly ClickHouseConnection connection;
    private readonly string viewName;
    private bool disposed;

    public LiveViewWatcher(ClickHouseConnection connection, string viewName)
    {
        this.connection = connection;
        this.viewName = viewName;
    }

    public delegate void EvChangeLiveViewResult(IList<dynamic> data);

    public event EvChangeLiveViewResult OnChangeLiveViewResult = data => { };

    /// <summary>
    /// Sets maximum number of updates to wait for
    /// NOTE: first update is output as soon as watch is started
    /// </summary>
    public uint? Limit { get; set; }

    public QueryStats QueryStats { get; private set; }

    public long Updates { get => Interlocked.Read(ref updates); }

    private long updates;

    public async Task WatchAsync()
    {
        var queryBuilder = new StringBuilder();
        queryBuilder.Append($"WATCH {viewName} ");
        if (Limit.HasValue)
            queryBuilder.Append($"LIMIT {Limit} ");
        queryBuilder.Append("FORMAT JSONEachRowWithProgress");

        var query = queryBuilder.ToString();

        var uriBuilder = connection.CreateUriBuilder();
        uriBuilder.CustomParameters.Add("query", query);

        using var httpClient = connection.GetHttpClient();

        using var request = new HttpRequestMessage(HttpMethod.Get, uriBuilder.ToString());
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        using var stream = await (await ClickHouseConnection.HandleError(response, query)).Content.ReadAsStreamAsync();
        using var streamReader = new StreamReader(stream, Encoding.UTF8);
        using var reader = new JsonTextReader(streamReader) { SupportMultipleContent = true, CloseInput = false };

        var contents = new List<dynamic>();
        while (reader.Read() && !cts.IsCancellationRequested)
        {
            var @object = JsonSettings.DefaultSerializer.Deserialize<JObject>(reader);
            var progressProperty = @object.Property("progress");
            if (progressProperty != null)
            {
                if (contents.Count > 0)
                {
                    Interlocked.Increment(ref updates);
                    OnChangeLiveViewResult(contents);
                    contents = new List<dynamic>();
                }
                QueryStats = progressProperty.Value.ToObject<QueryStats>(JsonSettings.SnakeCaseSerializer);
            }
            else
            {
                contents.Add(@object.Property("row"));
            }
        }
    }

    public async Task KeepWatchingAsync()
    {
        while (!disposed && !cts.IsCancellationRequested)
        {
            await WatchAsync();
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposed)
        {
            if (disposing)
            {
                cts.Cancel();
                cts.Dispose();
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            disposed = true;
        }
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

}
