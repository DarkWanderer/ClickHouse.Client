using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy
{
    public class ClickHouseBulkCopy : IDisposable
    {
        private readonly ClickHouseConnection connection;

        public ClickHouseBulkCopy(ClickHouseConnection connection)
        {
            this.connection = connection;
        }

        public int BatchSize { get; set; } = 50000;

        public string DestinationTableName { get; set; }

        public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

        public async Task WriteToServerAsync(IDataReader reader, CancellationToken token)
        {
            if (reader is null)
                throw new ArgumentNullException(nameof(reader));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);

            var tableColumns = await GetTargetTableSchemaAsync(token);

            var batch = new List<object[]>();

            async Task Flush()
            {
                await PushBatch(batch, token).ConfigureAwait(false);
                batch.Clear();
            }

            while (reader.Read())
            {
                token.ThrowIfCancellationRequested();
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                batch.Add(values);
                if (batch.Count >= BatchSize)
                    await Flush().ConfigureAwait(false);
            }
            await Flush().ConfigureAwait(false);
        }

        public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, CancellationToken.None);

        public async Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token)
        {
            if (rows is null)
                throw new ArgumentNullException(nameof(rows));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);

            var tableColumns = await GetTargetTableSchemaAsync(token);

            var batch = new List<object[]>();

            async Task Flush()
            {
                await PushBatch(batch, token).ConfigureAwait(false);
                batch.Clear();
            }

            foreach (var row in rows)
            {
                token.ThrowIfCancellationRequested();
                batch.Add(row);
                if (batch.Count >= BatchSize)
                    await Flush().ConfigureAwait(false);
            }
            await Flush().ConfigureAwait(false);
        }

        private async Task PushBatch(List<object[]> values, CancellationToken token)
        {
            var sb = new StringBuilder();
            foreach (var row in values)
            {
                sb.Append(string.Join("\t", row.Select(TabEscape)));
                sb.Append("\n");
            }

            var query = $"INSERT INTO {DestinationTableName} FORMAT TabSeparated";
            using var reader = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
            var result = await connection.PostDataAsync(query, reader, token).ConfigureAwait(false);
        }

        private async Task<ClickHouseType[]> GetTargetTableSchemaAsync(CancellationToken token)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT * FROM {DestinationTableName}";
            using var reader = (ClickHouseDataReader)await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, token).ConfigureAwait(false);
            return Enumerable.Range(0, reader.FieldCount).Select(reader.GetClickHouseType).ToArray();
        }

        private string TabEscape(object arg)
        {
            if (arg is null)
                return "\\N";

            switch (Type.GetTypeCode(arg.GetType()))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return Convert.ToString(arg, CultureInfo.InvariantCulture);
                case TypeCode.String:
                    return (arg as string).Replace("\t", "\\\t").Replace("\n", "\\\n").Replace("\\", "\\\\");
                case TypeCode.DateTime:
                    return ((DateTime)arg).ToString("yyyy-MM-dd hh:mm:ss");
                default:
                    return arg.ToString();
            }
        }

        private bool disposed = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                connection?.Dispose();
                disposed = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
