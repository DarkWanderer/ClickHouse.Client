using System;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
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
        private Uri serverUri;

        public ClickHouseConnection()
        {
            ConnectionString = "";  // Initialize with default values
        }

        public ClickHouseConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

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
                    Compression = useCompression
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
                Driver = builder.Driver;
            }
        }

        public override string Database => database;

        public override string DataSource { get; }

        public override string ServerVersion => serverVersion;

        internal async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
        {
            var uriBuilder = new UriBuilder(serverUri);
            var queryParameters = new HttpQueryParameters() { Database = database, Compress = useCompression };
            uriBuilder.Query = queryParameters.ToString();

            using var httpContent = new StringContent(sqlQuery);
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, uriBuilder.ToString());
            postMessage.Headers.Authorization = AuthenticationHeader;
            var response = await httpClient.PostAsync(serverUri, httpContent, token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw new ClickHouseServerException(error);
            }
            return response;
        }

        internal async Task<HttpResponseMessage> PostBulkDataAsync(string sql, Stream data, CancellationToken token)
        {
            var uriBuilder = new UriBuilder(serverUri);
            var queryParameters = new HttpQueryParameters()
            {
                SqlQuery = sql,
                Database = database,
                Compress = useCompression
            };
            uriBuilder.Query = queryParameters.ToString();

            using var httpContent = new StreamContent(data);
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, uriBuilder.ToString());
            postMessage.Headers.Authorization = AuthenticationHeader;

            var response = await httpClient.SendAsync(postMessage, token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw new ClickHouseServerException(error);
            }
            return response;
        }

        internal ClickHouseConnectionDriver Driver { get; private set; }

        public override ConnectionState State => state;

        private AuthenticationHeaderValue AuthenticationHeader => new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));

        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => throw new NotImplementedException();

        public override void Close() { }

        public override void Open() => OpenAsync().GetAwaiter().GetResult();

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

        private class HttpQueryParameters
        {
            private readonly NameValueCollection parameterCollection;

            public HttpQueryParameters() : this("") { }

            public HttpQueryParameters(string query)
            {
                parameterCollection = HttpUtility.ParseQueryString(query);
            }

            public string Database
            {
                get => parameterCollection.Get("database");
                set => parameterCollection.Set("database", value);
            }

            public bool Compress
            {
                get => parameterCollection.Get("compress") == "true";
                set => parameterCollection.Set("compress", value.ToString(CultureInfo.InvariantCulture));
            }

            public bool Decompress
            {
                get => parameterCollection.Get("decompress") == "true";
                set => parameterCollection.Set("decompress", value.ToString(CultureInfo.InvariantCulture));
            }

            public string SqlQuery
            {
                get => parameterCollection.Get("query");
                set => parameterCollection.Set("query", value);
            }

            public override string ToString() => Uri.EscapeUriString(HttpUtility.UrlDecode(parameterCollection.ToString()));
        }
    }
}
