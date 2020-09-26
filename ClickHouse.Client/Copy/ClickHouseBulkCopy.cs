using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
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
        private readonly string connectionString;
        private readonly int maxDegreeOfParallelism;
        private readonly IEnumerable<ClickHouseConnection> connections;
        private long rowsWritten = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseBulkCopy"/> class.
        /// </summary>
        /// <param name="connectionString">clickhouse db connection string</param>
        /// <param name="maxDegreeOfParallelism">Gets or sets maximum number of parallel processing tasks.</param>
        public ClickHouseBulkCopy(string connectionString, int maxDegreeOfParallelism = 4)
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            this.maxDegreeOfParallelism = maxDegreeOfParallelism;
            connections = InitializeConnections(maxDegreeOfParallelism);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseBulkCopy"/> class.
        /// </summary>
        /// <param name="connections">
        /// <see cref="ClickHouseBulkCopy"/> instances to be used for batched bulk operation.
        /// </param>
        /// <remarks>
        /// Bulk copy operation is parallelized by number of <paramref name="connections"/> provided.
        /// </remarks>
        public ClickHouseBulkCopy(IEnumerable<ClickHouseConnection> connections)
        {
            this.connections = connections ?? throw new ArgumentNullException(nameof(connections));
            maxDegreeOfParallelism = connections.Count();
        }

        /// <summary>
        /// Gets or sets size of batch in rows.
        /// </summary>
        public int BatchSize { get; set; } = 100000;

        /// <summary>
        /// Gets or sets name of destination table to insert to. "SELECT ..columns.. LIMIT 0" query is performed before insertion.
        /// </summary>
        public string DestinationTableName { get; set; }

        /// <summary>
        /// Gets total number of rows written by this instance.
        /// </summary>
        public long RowsWritten => Interlocked.Read(ref rowsWritten);

        public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

        public Task WriteToServerAsync(IDataReader reader, CancellationToken token)
        {
            if (reader is null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            return WriteToServerAsync(reader.AsEnumerable(), reader.GetColumnNames(), token);
        }

        public Task WriteToServerAsync(DataTable table, CancellationToken token)
        {
            if (table is null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            var rows = table.Rows.Cast<DataRow>().Select(r => r.ItemArray); // enumerable
            var columns = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
            return WriteToServerAsync(rows, columns, token);
        }

        public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, null, CancellationToken.None);

        public Task WriteToServerAsync(IEnumerable<object[]> rows, IReadOnlyCollection<string> columns) => WriteToServerAsync(rows, columns, CancellationToken.None);

        public Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token) => WriteToServerAsync(rows, null, token);

        public async Task WriteToServerAsync(IEnumerable<object[]> rows, IReadOnlyCollection<string> columns, CancellationToken token)
        {
            if (rows is null)
            {
                throw new ArgumentNullException(nameof(rows));
            }

            if (string.IsNullOrWhiteSpace(DestinationTableName))
            {
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);
            }

            ClickHouseType[] columnTypes = null;
            string[] columnNames = columns?.ToArray();

            using (var reader = (ClickHouseDataReader)await connections.FirstOrDefault()
                .ExecuteReaderAsync($"SELECT {GetColumnsExpression(columns)} FROM {DestinationTableName} LIMIT 0"))
            {
                columnTypes = reader.GetClickHouseColumnTypes();
                columnNames ??= reader.GetColumnNames();
            }

            var tasks = new (Task task, ClickHouseConnection connection)[connections.Count()];
            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = (Task.CompletedTask, connections.ElementAt(i));
            }
            foreach (var batch in rows.Batch(BatchSize))
            {
                token.ThrowIfCancellationRequested();
                while (true)
                {

                    var completedTaskIndex = Array.FindIndex(tasks, t => t.task.IsCompleted);
                    if (completedTaskIndex >= 0)
                    {
                        var completedTaskConnection = connections.ElementAt(completedTaskIndex);
                        // propagate exception if one happens
                        // 'await' instead of 'Wait()' to avoid dealing with AggregateException
                        await tasks[completedTaskIndex].task.ConfigureAwait(false);
                        var task = PushBatch(batch, columnTypes, columnNames, completedTaskConnection, token);
                        tasks[completedTaskIndex] = (task, completedTaskConnection);
                        break; // while (true); go to next batch
                    }
                    else
                    {
                        await Task.WhenAny(tasks.Select(e => e.task)).ConfigureAwait(false);
                    }
                }
            }
            await Task.WhenAll(tasks.Select(e => e.task)).ConfigureAwait(false);
        }

        private IEnumerable<ClickHouseConnection> InitializeConnections(int maxDegreeOfParallelism)
        {
            for (int index = 0; index < maxDegreeOfParallelism; index++)
            {
                yield return new ClickHouseConnection(connectionString);
            }
        }

        private string GetColumnsExpression(IReadOnlyCollection<string> columns) => columns == null || columns.Count == 0 ? "*" : string.Join(",", columns);

        private async Task PushBatch(ICollection<object[]> rows, ClickHouseType[] columnTypes, string[] columnNames, ClickHouseConnection connection, CancellationToken token)
        {
            using var stream = new MemoryStream() { Capacity = 512 * 1024 };
            using (var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024))
            {
                using var writer = new ExtendedBinaryWriter(gzipStream);
                using var streamer = new BinaryStreamWriter(writer);
                foreach (var row in rows)
                {
                    for (var i = 0; i < row.Length; i++)
                    {
                        streamer.Write(columnTypes[i], row[i]);
                    }
                }
            }
            stream.Seek(0, SeekOrigin.Begin);

            var query = $"INSERT INTO {DestinationTableName} ({string.Join(", ", columnNames)}) FORMAT RowBinary";
            await connection.PostBulkDataAsync(query, stream, true, token).ConfigureAwait(false);
            Interlocked.Add(ref rowsWritten, rows.Count);
        }
        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            foreach (var connection in connections)
            {
                connection?.Dispose();
            }
        }
    }
}
