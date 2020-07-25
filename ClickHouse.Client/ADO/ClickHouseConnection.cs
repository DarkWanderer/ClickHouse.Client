using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseConnection : DbConnection, ICloneable
    {
        private readonly HttpClient httpClient;

        private ConnectionState state = ConnectionState.Closed;
        private string serverVersion;
        private string database = "default";
        private string username;
        private string password;
        private bool useCompression;
        private string session;
        private TimeSpan timeout;
        private Uri serverUri;

        public ClickHouseConnection()
            : this(string.Empty)
        {
        }

        public ClickHouseConnection(string connectionString)
        {
            ConnectionString = connectionString;
            var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
            httpClient = new HttpClient(httpClientHandler, true)
            {
                Timeout = timeout,
            };
            // Connection string has to be initialized after HttpClient, as it may set HttpClient.Timeout
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseConnection"/> class using provided HttpClient.
        /// Note that HttpClient must have AutomaticDecompression enabled if compression is not disabled in connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="httpClient">instance of HttpClient</param>
        public ClickHouseConnection(string connectionString, HttpClient httpClient)
        {
            ConnectionString = connectionString;
            this.httpClient = httpClient;
        }

        /// <summary>
        /// Gets or sets string defining connection settings for ClickHouse server
        /// Example: Host=localhost;Port=8123;Username=default;Password=123;Compression=true
        /// </summary>
        public sealed override string ConnectionString
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
                    Compression = useCompression,
                    UseSession = session != null,
                    Timeout = timeout,
                };
                return builder.ToString();
            }

            set
            {
                var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = value };
                database = builder.Database;
                username = builder.Username;
                password = builder.Password;
                serverUri = new UriBuilder(builder.Protocol, builder.Host, builder.Port).Uri;
                useCompression = builder.Compression;
                session = builder.UseSession ? builder.SessionId ?? Guid.NewGuid().ToString() : null;
                timeout = builder.Timeout;
            }
        }

        public override string Database => database;

        public override string DataSource { get; }

        public override string ServerVersion => serverVersion;

        public override DataTable GetSchema() => GetSchema(null, null);

        public override DataTable GetSchema(string type) => GetSchema(type, null);

        public override DataTable GetSchema(string type, string[] restrictions) => SchemaDescriber.DescribeSchema(this, type, restrictions);

        internal async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token, ClickHouseParameterCollection parameters = null)
        {
            var uri = string.Empty;
            if (parameters == null)
            {
                uri = MakeUri();
            }
            else
            {
                var httpParametersSupported = await this.SupportsHttpParameters();

                var formattedParameters = new Dictionary<string, string>(parameters.Count);

                foreach (ClickHouseDbParameter parameter in parameters)
                {
                    var formattedParameter = httpParametersSupported
                        ? HttpParameterFormatter.Format(parameter)
                        : InlineParameterFormatter.Format(parameter);
                    formattedParameters.TryAdd(parameter.ParameterName, formattedParameter);
                }

                if (httpParametersSupported)
                {
                    uri = MakeUri(null, formattedParameters);
                }
                else
                {
                    sqlQuery = SubstituteParameters(sqlQuery, formattedParameters);
                    uri = MakeUri();
                }
            }

            using var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

            AddDefaultHttpHeaders(postMessage.Headers);
            HttpContent content = new StringContent(sqlQuery);
            content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
            if (useCompression)
            {
                content = new CompressedContent(content, DecompressionMethods.GZip);
            }

            postMessage.Content = content;

            var response = await httpClient.SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            return await HandleError(response, sqlQuery).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> PostBulkDataAsync(string sql, Stream data, bool isCompressed, CancellationToken token)
        {
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, MakeUri(sql));
            AddDefaultHttpHeaders(postMessage.Headers);

            postMessage.Content = new StreamContent(data);
            postMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (isCompressed)
            {
                postMessage.Content.Headers.Add("Content-Encoding", "gzip");
            }

            var response = await httpClient.SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            return await HandleError(response, sql).ConfigureAwait(false);
        }

        private static async Task<HttpResponseMessage> HandleError(HttpResponseMessage response, string query)
        {
            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw ClickHouseServerException.FromServerResponse(error, query);
            }
            return response;
        }

        // TODO: move this method out of ClickHouseConnection
        private string MakeUri(string sql = null, IDictionary<string, string> parameters = null)
        {
            var uriBuilder = new UriBuilder(serverUri);
            var queryParameters = new ClickHouseHttpQueryParameters()
            {
                Database = database,
                UseHttpCompression = useCompression,
                SqlQuery = sql,
                SessionId = session,
            };
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    queryParameters.SetParameter(parameter.Key, parameter.Value);
                }
            }

            uriBuilder.Query = queryParameters.ToString();
            return uriBuilder.ToString();
        }

        private static string SubstituteParameters(string query, IDictionary<string, string> parameters)
        {
            var builder = new StringBuilder(query.Length);

            var paramStartPos = query.IndexOf('{');
            var paramEndPos = -1;

            while (paramStartPos != -1)
            {
                builder.Append(query.Substring(paramEndPos + 1, paramStartPos - paramEndPos - 1));

                paramStartPos += 1;
                paramEndPos = query.IndexOf('}', paramStartPos);
                var param = query.Substring(paramStartPos, paramEndPos - paramStartPos);
                var delimiterPos = param.LastIndexOf(':');
                if (delimiterPos == -1)
                    throw new NotSupportedException($"param {param} doesn`t have data type");
                var name = param.Substring(0, delimiterPos);

                if (!parameters.TryGetValue(name, out var value))
                    throw new ArgumentOutOfRangeException($"Parameter {name} not found in parameters list");

                builder.Append(value);

                paramStartPos = query.IndexOf('{', paramEndPos);
            }

            builder.Append(query.Substring(paramEndPos + 1, query.Length - paramEndPos - 1));

            return builder.ToString();
        }

        public override ConnectionState State => state;

        private AuthenticationHeaderValue AuthenticationHeader => new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));

        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => new ClickHouseConnection(ConnectionString);

        public override void Close() => state = ConnectionState.Closed;

        public override void Open() => OpenAsync().ConfigureAwait(false).GetAwaiter().GetResult();

        public override async Task OpenAsync(CancellationToken token)
        {
            if (State == ConnectionState.Open)
                return;
            const string versionQuery = "SELECT version() FORMAT TSV";
            try
            {
                var response = await PostSqlQueryAsync(versionQuery, token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var data = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);

                if (data.Length > 2 && data[0] == 0x1F && data[1] == 0x8B) // Check if response starts with GZip marker
                    throw new InvalidOperationException("ClickHouse server returned compressed result but HttpClient did not decompress it");

                serverVersion = Encoding.UTF8.GetString(data).Trim();
                state = ConnectionState.Open;
            }
            catch
            {
                state = ConnectionState.Broken;
                throw;
            }
        }

        public new ClickHouseCommand CreateCommand() => new ClickHouseCommand(this);

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => CreateCommand();

        private void AddDefaultHttpHeaders(HttpRequestHeaders headers)
        {
            headers.Authorization = AuthenticationHeader;
            headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
            headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));
            if (useCompression)
            {
                headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
                headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            }
        }
    }
}
