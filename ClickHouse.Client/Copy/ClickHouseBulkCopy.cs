using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Copy
{
    public class ClickHouseBulkCopy : IDisposable
    {
        private readonly ClickHouseConnection connection;
        private long rowsWritten = 0;

        public ClickHouseBulkCopy(ClickHouseConnection connection)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public int BatchSize { get; set; } = 50000;

        public int MaxDegreeOfParallelism { get; set; } = 4;

        public string DestinationTableName { get; set; }

        public long RowsWritten => rowsWritten;

        public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

        public Task WriteToServerAsync(IDataReader reader, CancellationToken token)
        {
            if (reader is null)
                throw new ArgumentNullException(nameof(reader));

            return WriteToServerAsync(AsEnumerable(reader), reader.GetColumnNames(), token);
        }

        public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, null, CancellationToken.None);

        public Task WriteToServerAsync(IEnumerable<object[]> rows, IReadOnlyCollection<string> columns) => WriteToServerAsync(rows, columns, CancellationToken.None);

        public Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token) => WriteToServerAsync(rows, null, token);

        public async Task WriteToServerAsync(IEnumerable<object[]> rows, IReadOnlyCollection<string> columns, CancellationToken token)
        {
            if (rows is null)
                throw new ArgumentNullException(nameof(rows));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);

            ClickHouseType[] columnTypes = null;

            using (var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {GetColumnsExpression(columns)} FROM {DestinationTableName} LIMIT 0"))
            {
                columnTypes = Enumerable.Range(0, reader.FieldCount).Select(reader.GetClickHouseType).ToArray();
                columns = reader.GetColumnNames();
            }

            var tasks = new Task[MaxDegreeOfParallelism];
            for (var i = 0; i < tasks.Length; i++)
                tasks[i] = Task.CompletedTask;

            foreach (var batch in rows.Batch(BatchSize))
            {
                token.ThrowIfCancellationRequested();
                while (true)
                {
                    var completedTaskIndex = Array.FindIndex(tasks, t => t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Faulted || t.Status == TaskStatus.Canceled);
                    if (completedTaskIndex >= 0)
                    {
                        await tasks[completedTaskIndex]; // to receive exception if one happens
                        var task = PushBatch(batch, columnTypes, token);
                        tasks[completedTaskIndex] = task;
                        break;
                    }
                    else
                    {
                        await Task.WhenAny(tasks);
                    }
                }
            }
            await Task.WhenAll(tasks);
        }

        private string GetColumnsExpression(IReadOnlyCollection<string> columns) => columns == null || columns.Count == 0 ? "*" : string.Join(",", columns);

        private static IEnumerable<object[]> AsEnumerable(IDataReader reader)
        {
            while (reader.Read())
            {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                yield return values;
            }
        }

        private async Task PushBatch(ICollection<object[]> rows, ClickHouseType[] columnTypes, CancellationToken token)
        {
            using var stream = new MemoryStream() { Capacity = 256 * 1024 };
            using var writer = new ExtendedBinaryWriter(stream);
            using var streamer = new BinaryStreamWriter(writer);
            foreach (var row in rows)
            {
                for (var i = 0; i < row.Length; i++)
                {
                    streamer.WriteValue(row[i], columnTypes[i]);
                }
            }
            stream.Seek(0, SeekOrigin.Begin);

            var query = $"INSERT INTO {DestinationTableName} FORMAT RowBinary";
            await connection.PostDataAsync(query, stream, token).ConfigureAwait(false);
            Interlocked.Add(ref rowsWritten, rows.Count);
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
