using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO
{
    internal class ClickHouseCommand : DbCommand, IDisposable
    {
        private readonly ClickHouseConnection dbConnection;
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly ClickHouseParameterCollection clickHouseParameterCollection = new ClickHouseParameterCollection();

        public ClickHouseCommand(ClickHouseConnection connection)
        {
            dbConnection = connection;
        }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => dbConnection;
            set => throw new NotSupportedException();
        }


        protected override DbParameterCollection DbParameterCollection => clickHouseParameterCollection;

        protected override DbTransaction DbTransaction { get; set; }

        protected override bool CanRaiseEvents => base.CanRaiseEvents;

        public new void Dispose()
        {
            cts?.Dispose();
            base.Dispose();
        }

        public override void Cancel() => cts.Cancel();

        public override int ExecuteNonQuery() => ExecuteNonQueryAsync(cts.Token).GetAwaiter().GetResult();

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            var response = await dbConnection.PostSqlQueryAsync(CommandText, cts.Token, clickHouseParameterCollection).ConfigureAwait(false);
            var result = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return int.TryParse(result, out var r) ? r : 0;
        }

        public override object ExecuteScalar() => ExecuteScalarAsync(cts.Token).GetAwaiter().GetResult();

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
            return reader.Read() ? reader.GetValue(0) : null;
        }

        public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

        protected override DbParameter CreateDbParameter()
        {
            var parameter = new ClickHouseDbParameter();
            clickHouseParameterCollection.Add(parameter);
            return parameter;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                cts.Dispose();
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteDbDataReaderAsync(behavior, cts.Token).GetAwaiter().GetResult();

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var sqlBuilder = new StringBuilder(CommandText);
            var driver = dbConnection.Driver;
            switch (behavior)
            {
                case CommandBehavior.SingleRow:
                case CommandBehavior.SingleResult:
                    sqlBuilder.Append(" LIMIT 1");
                    break;
                case CommandBehavior.SchemaOnly:
                    sqlBuilder.Append(" LIMIT 0");
                    break;
                case CommandBehavior.CloseConnection:
                case CommandBehavior.Default:
                case CommandBehavior.KeyInfo:
                case CommandBehavior.SequentialAccess:
                    break;
            }
            switch (driver)
            {
                case ClickHouseConnectionDriver.Binary:
                    sqlBuilder.Append(" FORMAT RowBinaryWithNamesAndTypes");
                    break;
                case ClickHouseConnectionDriver.JSON:
                    sqlBuilder.Append(" FORMAT JSONCompact");
                    break;
                case ClickHouseConnectionDriver.TSV:
                    sqlBuilder.Append(" FORMAT TSVWithNamesAndTypes");
                    break;
            }

            var result = await dbConnection.PostSqlQueryAsync(sqlBuilder.ToString(), cts.Token, clickHouseParameterCollection).ConfigureAwait(false);

            return driver switch
            {
                ClickHouseConnectionDriver.Binary => new ClickHouseBinaryReader(result),
                ClickHouseConnectionDriver.JSON => new ClickHouseJsonCompactReader(result),
                ClickHouseConnectionDriver.TSV => new ClickHouseTsvReader(result),
                _ => throw new NotSupportedException("Unknown driver: " + driver.ToString()),
            };
        }
    }
}
