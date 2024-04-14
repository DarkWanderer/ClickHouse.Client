using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http.Headers;
using System.Net.Http;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Copy;

public class ClickHouseBulkCopy : IDisposable
{
    private readonly ClickHouseConnection connection;
    private readonly bool ownsConnection;
    private long rowsWritten;
    private (string[] names, ClickHouseType[] types) columnNamesAndTypes;

    public ClickHouseBulkCopy(ClickHouseConnection connection)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public ClickHouseBulkCopy(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));
        connection = new ClickHouseConnection(connectionString);
        ownsConnection = true;
    }

    /// <summary>
    /// Gets or sets size of batch in rows.
    /// </summary>
    public int BatchSize { get; set; } = 100000;

    /// <summary>
    /// Gets or sets maximum number of parallel processing tasks.
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 4;

    /// <summary>
    /// Gets name of destination table to insert to
    /// </summary>
    public string DestinationTableName { get; init; }

    /// <summary>
    /// Gets columns
    /// </summary>
    public IReadOnlyCollection<string> ColumnNames { get; init; }

    private async Task<(string[] names, ClickHouseType[] types)> LoadNamesAndTypesAsync(string destinationTableName, IReadOnlyCollection<string> columns = null)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {GetColumnsExpression(columns)} FROM {DestinationTableName} WHERE 1=0").ConfigureAwait(false);
        var types = reader.GetClickHouseColumnTypes();
        var names = reader.GetColumnNames().Select(c => c.EncloseColumnName()).ToArray();
        return (names, types);
    }

    /// <summary>
    /// Gets total number of rows written by this instance.
    /// </summary>
    public long RowsWritten => Interlocked.Read(ref rowsWritten);

    /// <summary>
    /// One-time init operation to load column types using provided names
    /// Required to call before WriteToServerAsync
    /// </summary>
    /// <returns>Awaitable task</returns>
    public async Task InitAsync()
    {
        if (DestinationTableName is null)
            throw new InvalidOperationException($"{nameof(DestinationTableName)} is null");
        columnNamesAndTypes = await LoadNamesAndTypesAsync(DestinationTableName, ColumnNames).ConfigureAwait(false);
    }

    public Task WriteToServerAsync(IDataReader reader) => WriteToServerAsync(reader, CancellationToken.None);

    public Task WriteToServerAsync(IDataReader reader, CancellationToken token)
    {
        if (reader is null)
            throw new ArgumentNullException(nameof(reader));

        return WriteToServerAsync(reader.AsEnumerable(), token);
    }

    public Task WriteToServerAsync(DataTable table, CancellationToken token)
    {
        if (table is null)
            throw new ArgumentNullException(nameof(table));

        var rows = table.Rows.Cast<DataRow>().Select(r => r.ItemArray);
        return WriteToServerAsync(rows, token);
    }

    public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, CancellationToken.None);

    public async Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token)
    {
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));

        if (string.IsNullOrWhiteSpace(DestinationTableName))
            throw new InvalidOperationException("Destination table not set");

        var (columnNames, columnTypes) = columnNamesAndTypes;
        if (columnNames == null || columnTypes == null)
            throw new InvalidOperationException("Column names not initialized. Call InitAsync once to load column data");

        var query = $"INSERT INTO {DestinationTableName} ({string.Join(", ", columnNames)}) FORMAT RowBinary";

        var tasks = new Task[MaxDegreeOfParallelism];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.CompletedTask;
        }

        foreach (var batch in IntoBatches(rows, query, columnTypes))
        {
            while (true)
            {
                var completedTaskIndex = Array.FindIndex(tasks, t => t.IsCompleted);
                if (completedTaskIndex >= 0)
                {
                    tasks[completedTaskIndex] = SendBatchAsync(batch, token);
                    break; // while (true); go to next batch
                }
                else
                {
                    var completedTask = await Task.WhenAny(tasks).ConfigureAwait(false);
                    await completedTask.ConfigureAwait(false);
                }
            }
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task SendBatchAsync(Batch batch, CancellationToken token)
    {
        // Async sending
        await connection.PostContentAsync(null, new BulkCopyHttpContent(batch, ColumnNames, true), token).ConfigureAwait(false);
        // Increase counter
        Interlocked.Add(ref rowsWritten, batch.Size);
    }

    public void Dispose()
    {
        if (ownsConnection)
        {
            connection?.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    private static string GetColumnsExpression(IReadOnlyCollection<string> columns) => columns == null || columns.Count == 0 ? "*" : string.Join(",", columns);

    private IEnumerable<Batch> IntoBatches(IEnumerable<object[]> rows, string query, ClickHouseType[] types)
    {
        foreach (var (batch, size) in rows.BatchRented(BatchSize))
        {
            yield return new Batch { Rows = batch, Size = size, Query = query, Types = types };
        }
    }

    // Convenience argument collection
    private struct Batch : IDisposable
    {
        public object[] Rows;
        public int Size;
        public string Query;
        public ClickHouseType[] Types;

        public void Dispose()
        {
            if (Rows != null)
            {
                ArrayPool<object>.Shared.Return(Rows);
                Rows = null;
            }
        }
    }

    private class BulkCopyHttpContent : HttpContent
    {
        private readonly Batch batch;
        private readonly IReadOnlyCollection<string> columnNames;
        private readonly bool isCompressed;

        public BulkCopyHttpContent(Batch batch, IReadOnlyCollection<string> columnNames, bool isCompressed)
        {
            this.batch = batch;
            this.columnNames = columnNames;
            this.isCompressed = isCompressed;
            Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (isCompressed)
            {
                Headers.ContentEncoding.Add("gzip");
            }
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (isCompressed)
            {
                using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, true))
                {
                    await SerializeBatchAsync(gzipStream);
                }
            }
            else
            {
                await SerializeBatchAsync(stream);
            }
        }

        private async Task SerializeBatchAsync(Stream stream)
        {
            using (var textWriter = new StreamWriter(stream, Encoding.UTF8, 4 * 1024, true))
            {
                await textWriter.WriteLineAsync(batch.Query);
            }

            using var writer = new ExtendedBinaryWriter(stream);

            int col = 0;
            object[] row = null;
            int counter = 0;
            var enumerator = batch.Rows.GetEnumerator();
            try
            {
                while (enumerator.MoveNext())
                {
                    row = (object[])enumerator.Current;
                    for (col = 0; col < columnNames.Count; col++)
                    {
                        batch.Types[col].Write(writer, row[col]);
                    }
                    counter++;
                    if (counter >= batch.Size)
                        break; // We've reached the batch size
                }
            }
            catch (Exception e)
            {
                throw new ClickHouseBulkCopySerializationException(row, col, e);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                batch.Dispose();
            base.Dispose(disposing);
        }
    }
}
