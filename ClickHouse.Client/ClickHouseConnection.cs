using System;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Threading;
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
                    Port = (ushort)serverUri?.Port,
                    Driver = Driver
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
                Driver = builder.Driver;
            }
        }

        public override string Database => database;

        public override string DataSource { get; }

        public override string ServerVersion => throw new NotImplementedException();

        internal async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
        {
            var httpContent = new StringContent(sqlQuery);
            return await httpClient.PostAsync(serverUri, httpContent, token);
        }

        internal ClickHouseConnectionDriver Driver { get; private set; }

        public override ConnectionState State => state;
        public override void ChangeDatabase(string databaseName) => database = databaseName;

        public object Clone() => throw new NotImplementedException();

        public override void Close() { }

        public override void Open() => OpenAsync().GetAwaiter().GetResult();

        public override async Task OpenAsync(CancellationToken token)
        {
            var response = await httpClient.GetAsync(serverUri, token);
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
