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
            //httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            //httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
            //httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));
            //httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            //httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            //httpClient.DefaultRequestHeaders.Authorization = AuthenticationHeader;
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

        internal async Task<HttpResponseMessage> GetSqlQueryAsync(string sql, CancellationToken token)
        {
            var response = await httpClient.GetAsync(MakeUri(sql), token).ConfigureAwait(false);
            return await HandleError(response).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
        {
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, MakeUri())
            {
                Content = new StringContent(sqlQuery)
            };
            var response = await httpClient.SendAsync(postMessage, token).ConfigureAwait(false);
            //var response = await httpClient.PostAsync(MakeUri(), new StringContent(sqlQuery), token);
            return await HandleError(response).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> PostDataAsync(string sql, Stream data, CancellationToken token)
        {
            using var postMessage = new HttpRequestMessage(HttpMethod.Post, MakeUri(sql));
            postMessage.Headers.Authorization = AuthenticationHeader;
            postMessage.Content = new StreamContent(data);

            var response = await httpClient.SendAsync(postMessage, token).ConfigureAwait(false);
            return await HandleError(response).ConfigureAwait(false);
        }

        private static async Task<HttpResponseMessage> HandleError(HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                throw new ClickHouseServerException(error);
            }
            return response;
        }

        private string MakeUri(string sql = null)
        {
            var uriBuilder = new UriBuilder(serverUri);
            var queryParameters = new HttpQueryParameters()
            {
                Database = database,
                // TODO - fix this. Bug in ClickHouse
                // Compress = useCompression
            };
            if (!string.IsNullOrWhiteSpace(sql))
                queryParameters.SqlQuery = sql;

            uriBuilder.Query = queryParameters.ToString();
            return uriBuilder.ToString();
        }

        internal ClickHouseConnectionDriver Driver { get; private set; }

        public override ConnectionState State => state;

        private AuthenticationHeaderValue AuthenticationHeader => new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));

        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => throw new NotImplementedException();

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
