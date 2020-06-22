using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO.Readers;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseCommand : DbCommand, IDisposable
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly ClickHouseParameterCollection clickHouseParameterCollection = new ClickHouseParameterCollection();
        private ClickHouseConnection connection;

        public ClickHouseCommand()
        {
        }

        public ClickHouseCommand(ClickHouseConnection connection)
        {
            this.connection = connection;
        }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => connection;
            set => connection = (ClickHouseConnection)value;
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

        public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new InvalidOperationException("Connection is not set");

            using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
            using var response = await connection.PostSqlQueryAsync(CommandText, linkedCancellationTokenSource.Token, clickHouseParameterCollection).ConfigureAwait(false);
            var result = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return int.TryParse(result, NumberStyles.Integer, CultureInfo.InvariantCulture, out var r) ? r : 0;
        }

        public override object ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
            using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, linkedCancellationTokenSource.Token).ConfigureAwait(false);
            return reader.Read() ? reader.GetValue(0) : null;
        }

        public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

        public new ClickHouseDbParameter CreateParameter() => (ClickHouseDbParameter)CreateDbParameter();

        protected override DbParameter CreateDbParameter() => new ClickHouseDbParameter();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                cts.Dispose();
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new InvalidOperationException("Connection is not set");

            using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
            var sqlBuilder = new StringBuilder(CommandText);
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
            sqlBuilder.Append(" FORMAT RowBinaryWithNamesAndTypes");
            var result = await connection.PostSqlQueryAsync(sqlBuilder.ToString(), linkedCancellationTokenSource.Token, clickHouseParameterCollection).ConfigureAwait(false);
            return new ClickHouseBinaryReader(result);
        }
    }
}
