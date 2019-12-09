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
using ClickHouse.Client.Properties;

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

        public async Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token)
        {
            if (rows is null)
                throw new ArgumentNullException(nameof(rows));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);

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
                sb.AppendLine(string.Join("\t", row.Select(TabEscape)));

            var query = $"INSERT INTO {DestinationTableName} FORMAT TabSeparated";
            using var reader = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
            await connection.PostBulkDataAsync(query, reader, token).ConfigureAwait(false);
        }

        private string TabEscape(object arg) 
        {
            if (arg is int || arg is float || arg is double || arg is long)
                return Convert.ToString(arg, CultureInfo.InvariantCulture);
            if (arg is string s)
                return s.Replace("\t", "\\\t").Replace("\n", "\\\n").Replace("\\", "\\\\");
            if (arg is DateTime dt)
                return dt.ToString("yyyy-MM-dd hh:mm:ss");
            if (arg is null)
                return "\\N";
            return arg.ToString();
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
