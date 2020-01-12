using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Utility
{
    public static class ConnectionExtensions
    {
        public static async Task<int> ExecuteStatementAsync(this ClickHouseConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        public static async Task<object> ExecuteScalarAsync(this ClickHouseConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return await command.ExecuteScalarAsync().ConfigureAwait(false);
        }

        public static async Task<DbDataReader> ExecuteReaderAsync(this ClickHouseConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return await command.ExecuteReaderAsync().ConfigureAwait(false);
        }
    }
}
