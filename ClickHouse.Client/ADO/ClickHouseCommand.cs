using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseCommand : DbCommand, IClickHouseCommand, IDisposable
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly ClickHouseParameterCollection commandParameters = new ClickHouseParameterCollection();
        private readonly IDictionary<string, string> queryParameters = new Dictionary<string, string>();
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

        protected override DbParameterCollection DbParameterCollection => commandParameters;

        protected override DbTransaction DbTransaction { get; set; }

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
            using var response = await connection.PostSqlQueryAsync(CommandText, linkedCancellationTokenSource.Token, commandParameters, this.queryParameters).ConfigureAwait(false);
            try
            {
                using var reader = new ExtendedBinaryReader(await response.Content.ReadAsStreamAsync());
                return reader.Read7BitEncodedInt();
            }
            catch
            {
                return 0;
            }
        }

        /// <inheritdoc />
        public async Task<ClickHouseRawResult> ExecuteRawResultAsync(CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new InvalidOperationException("Connection is not set");

            using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
            var response = await connection.PostSqlQueryAsync(CommandText, linkedCancellationTokenSource.Token, commandParameters, this.queryParameters).ConfigureAwait(false);
            return new ClickHouseRawResult(response);
        }

        public override object ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
            using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, linkedCancellationTokenSource.Token).ConfigureAwait(false);
            return reader.Read() ? reader.GetValue(0) : null;
        }

        public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

        /// <inheritdoc />
        public new ClickHouseDbParameter CreateParameter() => new ClickHouseDbParameter();

        /// <inheritdoc />
        public void SetQueryParameter(string name, string value)
        {
            if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(value))
            {
                throw new ArgumentException($"The '{nameof(name)}' and '{nameof(value)}' parameters should not be null or consisting of white-spaces only.");
            }

            this.queryParameters[name] = value;
        }

        /// <inheritdoc />
        public void RemoveQueryParameter(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException($"The '{nameof(name)}' parameter should not be null or consisting of white-spaces only.");
            }

            this.queryParameters.Remove(name);
        }

        protected override DbParameter CreateDbParameter() => CreateParameter();

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
                default:
                    break;
            }
            var result = await connection.PostSqlQueryAsync(sqlBuilder.ToString(), linkedCancellationTokenSource.Token, commandParameters, this.queryParameters).ConfigureAwait(false);
            return new ClickHouseDataReader(result);
        }
    }
}
