using System;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseConnection : DbConnection, ICloneable
    {
        private static readonly HttpClientHandler handler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
        private static readonly HttpClient httpClient = new HttpClient(handler);

        private ConnectionState state = ConnectionState.Closed;
        private string serverVersion;
        private string database = "default";
        private string username;
        private string password;
        private bool useCompression;
        private string session;
        private Uri serverUri;

        public ClickHouseConnection()
        {
            ConnectionString = "";  // Initialize with default values
        }

        public ClickHouseConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// String defining connection settings for ClickHouse server
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
                    Driver = Driver,
                    Compression = useCompression,
                    UseSession = session != null
                };
                return builder.ToString();
            }

            set
            {
                var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = value };
                database = builder.Database;
                username = builder.Username;
                password = builder.Password;
                serverUri = new UriBuilder("http", builder.Host, builder.Port).Uri;
                useCompression = builder.Compression;
                session = builder.UseSession ? Guid.NewGuid().ToString() : null;
                Driver = builder.Driver;
            }
        }

        public override string Database => database;

        public override string DataSource { get; }

        public override string ServerVersion => serverVersion;

        internal async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
        {
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, MakeUri());

            AddDefaultHttpHeaders(postMessage.Headers);
            if (useCompression)
            {
                var data = new MemoryStream(Encoding.UTF8.GetBytes(sqlQuery));

                using var compressedStream = new MemoryStream();

                using (var gzipStream = new GZipStream(compressedStream, CompressionLevel.Fastest, true))
                    await data.CopyToAsync(gzipStream).ConfigureAwait(false);

                postMessage.Content = new ByteArrayContent(compressedStream.ToArray());
                postMessage.Content.Headers.Add("Content-Encoding", "gzip");
            }
            else
            {
                postMessage.Content = new StringContent(sqlQuery);
            }

            postMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
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
                postMessage.Content.Headers.Add("Content-Encoding", "gzip");

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

        private string MakeUri(string sql = null)
        {
            var uriBuilder = new UriBuilder(serverUri);
            var queryParameters = new HttpQueryParameters()
            {
                Database = database,
                UseHttpCompression = useCompression,
                SqlQuery = sql,
                SessionId = session
            };

            uriBuilder.Query = queryParameters.ToString();
            return uriBuilder.ToString();
        }

        internal ClickHouseConnectionDriver Driver { get; private set; }

        public override ConnectionState State => state;

        private AuthenticationHeaderValue AuthenticationHeader => new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));

        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => new ClickHouseConnection(ConnectionString);

        public override void Close() => state = ConnectionState.Closed;

        public override void Open() => OpenAsync().ConfigureAwait(false).GetAwaiter().GetResult();

        public override async Task OpenAsync(CancellationToken token)
        {
            try
            {
                var response = await PostSqlQueryAsync("SELECT version()", token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                serverVersion = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                state = ConnectionState.Open;
            }
            catch
            {
                state = ConnectionState.Broken;
                throw;
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => new ClickHouseCommand(this);

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

        private class HttpQueryParameters
        {
            private readonly NameValueCollection parameterCollection;

            public HttpQueryParameters() : this("") { }

            public HttpQueryParameters(string query)
            {
                parameterCollection = HttpUtility.ParseQueryString(query);
                // Do not put quotes around 64-bit integers
                parameterCollection.Set("output_format_json_quote_64bit_integers", false.ToString());
            }

            public string Database
            {
                get => parameterCollection.Get("database");
                set => parameterCollection.Set("database", value);
            }

            public bool UseHttpCompression
            {
                get => parameterCollection.Get("enable_http_compression").Equals("true", StringComparison.OrdinalIgnoreCase);
                set => parameterCollection.Set("enable_http_compression", value.ToString(CultureInfo.InvariantCulture));
            }

            public string SqlQuery
            {
                get => parameterCollection.Get("query");
                set => SetOrRemove("query", value);
            }

            public string SessionId
            {
                get => parameterCollection.Get("session_id");
                set => SetOrRemove("session_id", value);
            }

            private void SetOrRemove(string name, string value)
            {
                if (!string.IsNullOrEmpty(value))
                    parameterCollection.Set(name, value);
                else
                    parameterCollection.Remove(name);
            }

            public override string ToString() => Uri.EscapeUriString(HttpUtility.UrlDecode(parameterCollection.ToString()));
        }
    }
}
