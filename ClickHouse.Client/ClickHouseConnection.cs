using System;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Client
{
    public class ClickHouseConnection : DbConnection, ICloneable, IDisposable
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private ConnectionState state = ConnectionState.Closed;
        private string database;
        private string username;
        private string password;
        private Uri serverUri;

        public ClickHouseConnection()
        {
            ConnectionString = "";  // Initialize with default values
        }

        public ClickHouseConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString
        {
            get
            {
                var builder = new ClickHouseConnectionStringBuilder
                {
                    Database = database,
                    Username = username,
                    Password = password,
                    Host = serverUri?.Host,
                    Port = (ushort)serverUri?.Port
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

            }
        }

        public override string Database => database;

        public override string DataSource { get; }

        public override string ServerVersion
        {
            get
            {
                return PostSqlQueryAsync("select version();").GetAwaiter().GetResult();
            }
        }

        private async Task<string> PostSqlQueryAsync(string sqlQuery)
        {
            var httpContent = new StringContent(sqlQuery);
            var response = await httpClient.PostAsync(serverUri, httpContent);
            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadAsStringAsync();
            return result;
        }

        public override ConnectionState State => state;
        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => throw new NotImplementedException();

        public override void Close() { }

        public override void Open() => OpenAsync().GetAwaiter().GetResult();

        public new async Task OpenAsync()
        {
            var response = await httpClient.GetAsync(serverUri);
            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadAsStringAsync();
            if (result.ToLowerInvariant().StartsWith("ok"))
            {
                state = ConnectionState.Open;
            }
            else
            {
                state = ConnectionState.Broken;
                throw new ClickHouseServerException("Invalid handshake, got " + result);
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => new ClickHouseCommand(this);
    }
}
