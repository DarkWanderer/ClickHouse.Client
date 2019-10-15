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

        public override string ServerVersion { get; }

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
            if (result == "Ok.")
            {
                state = ConnectionState.Open;
            }
            else
            {
                state = ConnectionState.Broken;
                throw new ClickHouseServerException("Invalid handshake");
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => new ClickHouseCommand(this);
    }
}
