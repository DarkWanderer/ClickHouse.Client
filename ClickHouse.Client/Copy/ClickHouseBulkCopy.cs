using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Copy.Serializer;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;
using Microsoft.IO;

namespace ClickHouse.Client.Copy;

public class ClickHouseBulkCopy : IDisposable
{
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();
    private readonly ClickHouseConnection connection;
    private readonly BatchSerializer batchSerializer;
    private readonly RowBinaryFormat rowBinaryFormat;
    private readonly bool ownsConnection;
    private long rowsWritten;
    private (string[] names, ClickHouseType[] types) columnNamesAndTypes;

    public ClickHouseBulkCopy(ClickHouseConnection connection)
        : this(connection, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(string connectionString)
        : this(connectionString, RowBinaryFormat.RowBinary) { }

    public ClickHouseBulkCopy(ClickHouseConnection connection, RowBinaryFormat rowBinaryFormat)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        this.rowBinaryFormat = rowBinaryFormat;
        batchSerializer = BatchSerializer.GetByRowBinaryFormat(rowBinaryFormat);
    }

    public ClickHouseBulkCopy(string connectionString, RowBinaryFormat rowBinaryFormat)
        : this(
            string.IsNullOrWhiteSpace(connectionString)
                ? throw new ArgumentNullException(nameof(connectionString))
                : new ClickHouseConnection(connectionString),
            rowBinaryFormat)
    {
        ownsConnection = true;
    }

    /// <summary>
    /// Bulk insert progress event.
    /// </summary>
    public event EventHandler<BatchSentEventArgs> BatchSent;

    /// <summary>
    /// Gets or sets size of batch in rows.
    /// </summary>
    public int BatchSize { get; set; } = 100000;

    /// <summary>
    /// Gets or sets maximum number of parallel processing tasks.
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 4;

    /// <summary>
    /// Gets name of destination table to insert to.
    /// </summary>
    public string DestinationTableName { get; init; }

    /// <summary>
    /// Gets columns
    /// </summary>
    public IReadOnlyCollection<string> ColumnNames { get; init; }

    public sealed class BatchSentEventArgs : EventArgs
    {
        internal BatchSentEventArgs(long rowsWritten)
        {
            RowsWritten = rowsWritten;
        }

        public long RowsWritten
        {
            get;
        }
    }

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

        return WriteToServerAsync(reader.AsEnumerable().Select(r => new Memory<object>(r)), token);
    }

    public Task WriteToServerAsync(DataTable table, CancellationToken token)
    {
        if (table is null)
            throw new ArgumentNullException(nameof(table));

        var rows = table.Rows.Cast<DataRow>().Select(r => new Memory<object>(r.ItemArray));
        return WriteToServerAsync(rows, token);
    }

    public Task WriteToServerAsync(IEnumerable<object[]> rows) =>
        WriteToServerAsync(rows.Select(r => new Memory<object>(r)), CancellationToken.None);

    public Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token) =>
        WriteToServerAsync(rows.Select(r => new Memory<object>(r)), token);

    public async Task WriteToServerAsync(IEnumerable<Memory<object>> rows, CancellationToken token)
    {
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));

        if (string.IsNullOrWhiteSpace(DestinationTableName))
            throw new InvalidOperationException("Destination table not set");

        var (columnNames, columnTypes) = columnNamesAndTypes;
        if (columnNames == null || columnTypes == null)
            throw new InvalidOperationException("Column names not initialized. Call InitAsync once to load column data");

        var query = $"INSERT INTO {DestinationTableName} ({string.Join(", ", columnNames)}) FORMAT {rowBinaryFormat.ToString()}";

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
        using (batch) // Dispose object regardless whether sending succeeds
        {
            using var stream = MemoryStreamManager.GetStream(nameof(SendBatchAsync));
            // Async serialization
            await Task.Run(() => batchSerializer.Serialize(batch, stream), token).ConfigureAwait(false);
            // Seek to beginning as after writing it's at end
            stream.Seek(0, SeekOrigin.Begin);
            // Async sending
            await connection.PostStreamAsync(null, stream, true, token).ConfigureAwait(false);
            // Increase counter
            var batchRowsWritten = Interlocked.Add(ref rowsWritten, batch.Size);
            // Raise BatchSent event
            BatchSent?.Invoke(this, new BatchSentEventArgs(batchRowsWritten));
        }
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

    private IEnumerable<Batch> IntoBatches(IEnumerable<Memory<object>> rows, string query, ClickHouseType[] types)
    {
        foreach (var (batch, size) in rows.BatchRented(BatchSize))
        {
            yield return new Batch { Rows = batch, Size = size, Query = query, Types = types };
        }
    }
}
