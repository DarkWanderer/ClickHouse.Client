using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;

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

        public TimeSpan BulkCopyTimeout { get; set; }

        public string DestinationTableName { get; set; }

        public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

        public async Task WriteToServerAsync(IDataReader reader, CancellationToken token)
        {
            if (reader is null)
                throw new ArgumentNullException(nameof(reader));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException("Destination table not set");

            var batch = new List<object[]>();

            async Task Flush()
            { 
                await PushBatch(batch, token).ConfigureAwait(false); 
                batch.Clear(); 
            }

            while (reader.Read())
            {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                batch.Add(values);
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
                sb.AppendJoin('\t', row);
                sb.AppendLine();
            }

            var query = $"INSERT INTO {DestinationTableName} FORMAT TabSeparated";
            using var reader = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
            await connection.PostBulkDataAsync(query, reader, token).ConfigureAwait(false);
        }

        private bool disposed = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    connection?.Dispose();
                }
                disposed = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }
    }
}
