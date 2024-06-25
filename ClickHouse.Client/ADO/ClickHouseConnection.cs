﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.Diagnostic;
using ClickHouse.Client.Http;
using ClickHouse.Client.Utility;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Client.ADO;

public class ClickHouseConnection : DbConnection, IClickHouseConnection, ICloneable, IDisposable
{
    private const string CustomSettingPrefix = "set_";

    private readonly List<IDisposable> disposables = new();
    private readonly string httpClientName;
    private readonly ConcurrentDictionary<string, object> customSettings = new ConcurrentDictionary<string, object>();
    private volatile ConnectionState state = ConnectionState.Closed; // Not an autoproperty because of interface implementation

    // Values provided by constructor
    private HttpClient providedHttpClient;
    private IHttpClientFactory providedHttpClientFactory;
    // Actually used value
    private IHttpClientFactory httpClientFactory;

    private Version serverVersion;
    private string serverTimezone;

    private string database = "default";
    private string username;
    private string password;
    private string session;
    private bool useServerTimezone;
    private bool useCustomDecimals;
    private TimeSpan timeout;
    private Uri serverUri;
    private Feature supportedFeatures;

    public ClickHouseConnection()
        : this(string.Empty)
    {
    }

    public ClickHouseConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnection"/> class using provided HttpClient.
    /// Note that HttpClient must have AutomaticDecompression enabled if compression is not disabled in connection string
    /// </summary>
    /// <param name="connectionString">Connection string</param>
    /// <param name="httpClient">instance of HttpClient</param>
    public ClickHouseConnection(string connectionString, HttpClient httpClient)
    {
        providedHttpClient = httpClient;
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnection"/> class using an HttpClient generated by the provided <paramref name="httpClientFactory"/>.
    /// </summary>
    /// <param name="connectionString">The ClickHouse connection string.</param>
    /// <param name="httpClientFactory">The factory to be used for creating the clients.</param>
    /// <param name="httpClientName">
    /// The name of the HTTP client you want to be created using the provided factory.
    /// If left empty, the default client will be created.
    /// </param>
    /// <remarks>
    /// <list type="bullet">
    /// <item>
    /// If compression is not disabled in the <paramref name="connectionString"/>, the <paramref name="httpClientFactory"/>
    /// must be configured to enable <see cref="HttpClientHandler.AutomaticDecompression"/> for its generated clients.
    /// <example>
    /// For example you can do this while registering the HTTP client:
    /// <code>
    /// services.AddHttpClient("ClickHouseClient").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
    /// {
    ///     AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    /// });
    /// </code>
    /// </example>
    /// </item>
    /// <item>
    /// The <paramref name="httpClientFactory"/> must set the timeout for its clients if needed.
    /// <example>
    /// For example you can do this while registering the HTTP client:
    /// <code>
    /// services.AddHttpClient("ClickHouseClient", c => c.Timeout = TimeSpan.FromMinutes(5));
    /// </code>
    /// </example>
    /// </item>
    /// </list>
    /// </remarks>
    public ClickHouseConnection(string connectionString, IHttpClientFactory httpClientFactory, string httpClientName = "")
    {
        this.providedHttpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        this.httpClientName = httpClientName ?? throw new ArgumentNullException(nameof(httpClientName));
        ConnectionString = connectionString;
    }

    public ILogger Logger { get; set; }

    /// <summary>
    /// Gets or sets string defining connection settings for ClickHouse server
    /// Example: Host=localhost;Port=8123;Username=default;Password=123;Compression=true
    /// </summary>
    public sealed override string ConnectionString
    {
        get => ConnectionStringBuilder.ToString();
        set => ConnectionStringBuilder = new ClickHouseConnectionStringBuilder() { ConnectionString = value };
    }

    public IDictionary<string, object> CustomSettings => customSettings;

    public override ConnectionState State => state;

    public override string Database => database;

    internal string Username => username;

    internal Uri ServerUri => serverUri;

    internal string RedactedConnectionString
    {
        get
        {
            var builder = ConnectionStringBuilder;
            builder.Password = "****";
            return builder.ToString();
        }
    }

    public string ServerTimezone => serverTimezone;

    public override string DataSource { get; }

    public override string ServerVersion => serverVersion?.ToString();

    public bool UseCompression { get; private set; }

    /// <summary>
    /// Gets enum describing which ClickHouse features are available on this particular server version
    /// Requires connection to be in Open state
    /// </summary>
    public virtual Feature SupportedFeatures
    {
        get => state == ConnectionState.Open ? supportedFeatures : throw new InvalidOperationException();
        private set => supportedFeatures = value;
    }

    private void ResetHttpClientFactory()
    {
        // If current httpClientFactory is owned by this connection, dispose of it
        if (httpClientFactory is IDisposable d && disposables.Contains(d))
        {
            d.Dispose();
            disposables.Remove(d);
        }

        // If we have a HttpClient provided, use it
        if (providedHttpClient != null)
        {
            httpClientFactory = new CannedHttpClientFactory(providedHttpClient);
        }

        // If we have a provided client factory, use that
        else if (providedHttpClientFactory != null)
        {
            httpClientFactory = providedHttpClientFactory;
        }

        // If sessions are enabled, always use single connection
        else if (!string.IsNullOrEmpty(session))
        {
            var factory = new SingleConnectionHttpClientFactory() { Timeout = timeout };
            disposables.Add(factory);
            httpClientFactory = factory;
        }

        // Default case - use default connection pool
        else
        {
            httpClientFactory = new DefaultPoolHttpClientFactory() { Timeout = timeout };
        }
    }

    public override DataTable GetSchema() => GetSchema(null, null);

    public override DataTable GetSchema(string collectionName) => GetSchema(collectionName, null);

    public override DataTable GetSchema(string collectionName, string[] restrictionValues) => SchemaDescriber.DescribeSchema(this, collectionName, restrictionValues);

    internal static async Task<HttpResponseMessage> HandleError(HttpResponseMessage response, string query, Activity activity)
    {
        if (response.IsSuccessStatusCode)
        {
            activity.SetSuccess();
            return response;
        }
        var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var ex = ClickHouseServerException.FromServerResponse(error, query);
        activity.SetException(ex);
        throw ex;
    }

    public override void ChangeDatabase(string databaseName) => database = databaseName;

    public object Clone() => new ClickHouseConnection(ConnectionString);

    public override void Close() => state = ConnectionState.Closed;

    public override void Open() => OpenAsync().ConfigureAwait(false).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        const string versionQuery = "SELECT version(), timezone() FORMAT TSV";

        if (State == ConnectionState.Open)
            return;
        using var activity = this.StartActivity("OpenAsync");
        activity.SetQuery(versionQuery);

        try
        {
            var uriBuilder = CreateUriBuilder();
            var request = new HttpRequestMessage(HttpMethod.Post, uriBuilder.ToString())
            {
                Content = new StringContent(versionQuery, Encoding.UTF8),
            };
            AddDefaultHttpHeaders(request.Headers);
            var response = await HandleError(await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false), versionQuery, activity).ConfigureAwait(false);
#if NET5_0_OR_GREATER
            var data = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
#else
            var data = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
#endif

            if (data.Length > 2 && data[0] == 0x1F && data[1] == 0x8B) // Check if response starts with GZip marker
                throw new InvalidOperationException("ClickHouse server returned compressed result but HttpClient did not decompress it. Check HttpClient settings");

            if (data.Length == 0)
                throw new InvalidOperationException("ClickHouse server did not return version, check if the server is functional");

            var serverVersionAndTimezone = Encoding.UTF8.GetString(data).Trim().Split('\t');

            serverVersion = ParseVersion(serverVersionAndTimezone[0]);
            serverTimezone = serverVersionAndTimezone[1];
            SupportedFeatures = ClickHouseFeatureMap.GetFeatureFlags(serverVersion);
            state = ConnectionState.Open;
        }
        catch (Exception)
        {
            state = ConnectionState.Broken;
            throw;
        }
    }

    /// <summary>
    /// Warning: implementation-specific API. Exposed to allow custom optimizations
    /// May change in future versions
    /// </summary>
    /// <param name="sql">SQL query to add to URL, may be empty</param>
    /// <param name="data">Raw stream to be sent. May contain SQL query at the beginning. May be gzip-compressed</param>
    /// <param name="isCompressed">indicates whether "Content-Encoding: gzip" header should be added</param>
    /// <param name="token">Cancellation token</param>
    /// <returns>Task-wrapped HttpResponseMessage object</returns>
    public async Task PostStreamAsync(string sql, Stream data, bool isCompressed, CancellationToken token)
    {
        var content = new StreamContent(data);
        await PostStreamAsync(sql, content, isCompressed, token).ConfigureAwait(false);;
    }

    /// <summary>
    /// Warning: implementation-specific API. Exposed to allow custom optimizations
    /// May change in future versions
    /// </summary>
    /// <param name="sql">SQL query to add to URL, may be empty</param>
    /// <param name="callback">Callback invoked to write to the stream. May contain SQL query at the beginning. May be gzip-compressed</param>
    /// <param name="isCompressed">indicates whether "Content-Encoding: gzip" header should be added</param>
    /// <param name="token">Cancellation token</param>
    /// <returns>Task-wrapped HttpResponseMessage object</returns>
    public async Task PostStreamAsync(string sql, Func<Stream, CancellationToken, Task> callback, bool isCompressed, CancellationToken token)
    {
        var content = new StreamCallbackContent(callback, token);
        await PostStreamAsync(sql, content, isCompressed, token).ConfigureAwait(false);
    }

    private async Task PostStreamAsync(string sql, HttpContent content, bool isCompressed, CancellationToken token)
    {
        using var activity = this.StartActivity("PostStreamAsync");
        activity.SetQuery(sql);

        var builder = CreateUriBuilder(sql);
        using var postMessage = new HttpRequestMessage(HttpMethod.Post, builder.ToString());
        AddDefaultHttpHeaders(postMessage.Headers);

        postMessage.Content = content;
        postMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (isCompressed)
        {
            postMessage.Content.Headers.Add("Content-Encoding", "gzip");
        }
        using var response = await HttpClient.SendAsync(postMessage, HttpCompletionOption.ResponseContentRead, token).ConfigureAwait(false);
        await HandleError(response, sql, activity).ConfigureAwait(false);
    }

    public new ClickHouseCommand CreateCommand() => new ClickHouseCommand(this);

    void IDisposable.Dispose()
    {
        GC.SuppressFinalize(this);
        foreach (var d in disposables)
            d.Dispose();
    }

    internal static Version ParseVersion(string versionString)
    {
        if (string.IsNullOrWhiteSpace(versionString))
            throw new ArgumentException($"'{nameof(versionString)}' cannot be null or whitespace.", nameof(versionString));
        var parts = versionString.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) ? i : 0)
            .ToArray();
        if (parts.Length == 0 || parts[0] == 0)
            throw new InvalidOperationException($"Invalid version: {versionString}");
        return new Version(parts.ElementAtOrDefault(0), parts.ElementAtOrDefault(1), parts.ElementAtOrDefault(2), parts.ElementAtOrDefault(3));
    }

    internal HttpClient HttpClient => httpClientFactory.CreateClient(httpClientName);

    internal TypeSettings TypeSettings => new TypeSettings(useCustomDecimals, useServerTimezone ? serverTimezone : TypeSettings.DefaultTimezone);

    internal ClickHouseUriBuilder CreateUriBuilder(string sql = null) => new ClickHouseUriBuilder(serverUri)
    {
        Database = database,
        SessionId = session,
        UseCompression = UseCompression,
        CustomParameters = customSettings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
        Sql = sql,
    };

    internal Task EnsureOpenAsync() => state != ConnectionState.Open ? OpenAsync() : Task.CompletedTask;

    internal void AddDefaultHttpHeaders(HttpRequestHeaders headers)
    {
        headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));
        if (UseCompression)
        {
            headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
        }
    }

    internal ClickHouseConnectionStringBuilder ConnectionStringBuilder
    {
        get
        {
            var builder = new ClickHouseConnectionStringBuilder
            {
                Database = database,
                Username = username,
                Password = password,
                Host = serverUri?.Host,
                Port = (ushort)serverUri?.Port,
                Compression = UseCompression,
                UseSession = session != null,
                Timeout = timeout,
                UseServerTimezone = useServerTimezone,
                UseCustomDecimals = useCustomDecimals,
            };

            foreach (var kvp in CustomSettings)
                builder[CustomSettingPrefix + kvp.Key] = kvp.Value;

            return builder;
        }

        set
        {
            var builder = value;
            database = builder.Database;
            username = builder.Username;
            password = builder.Password;
            serverUri = new UriBuilder(builder.Protocol, builder.Host, builder.Port).Uri;
            UseCompression = builder.Compression;
            session = builder.UseSession ? builder.SessionId ?? Guid.NewGuid().ToString() : null;
            timeout = builder.Timeout;
            useServerTimezone = builder.UseServerTimezone;
            useCustomDecimals = builder.UseCustomDecimals;

            foreach (var key in builder.Keys.Cast<string>().Where(k => k.StartsWith(CustomSettingPrefix, true, CultureInfo.InvariantCulture)))
            {
                CustomSettings.Set(key.Replace(CustomSettingPrefix, string.Empty), builder[key]);
            }

            ResetHttpClientFactory();
        }
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

    protected override DbCommand CreateDbCommand() => CreateCommand();
}
