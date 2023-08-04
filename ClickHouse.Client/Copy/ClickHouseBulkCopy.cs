using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
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
    private bool ownsConnection;
    private long rowsWritten;

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
            throw new InvalidOperationException("Destination table not set");
        }

        ClickHouseType[] columnTypes = null;
        string[] columnNames = columns?.ToArray();

        using (var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT {ClickHouseBulkCopy.GetColumnsExpression(columns)} FROM {DestinationTableName} WHERE 1=0").ConfigureAwait(false))
        {
            columnTypes = reader.GetClickHouseColumnTypes();
            columnNames ??= reader.GetColumnNames();
        }
        for (int i = 0; i < columnNames.Length; i++)
            columnNames[i] = columnNames[i].EncloseColumnName();

        var tasks = new Task[MaxDegreeOfParallelism];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.CompletedTask;
        }

        var query = $"INSERT INTO {DestinationTableName} ({string.Join(", ", columnNames)}) FORMAT RowBinary";
        bool useInlineQuery = connection.SupportedFeatures.HasFlag(Feature.InlineQuery);

        // Variables are outside the loop to capture context in case of exception
        object[] row = null;
        int col = 0;
        var enumerator = rows.GetEnumerator();
        bool hasMore = false;
        do
        {
            token.ThrowIfCancellationRequested();
            var stream = new MemoryStream() { Capacity = 4 * 1024 };
            int counter = 0;
            using (var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024))
            {
                if (useInlineQuery)
                {
                    using var textWriter = new StreamWriter(gzipStream, Encoding.UTF8, 4 * 1024, true);
                    textWriter.WriteLine(query);
                }

                using var writer = new ExtendedBinaryWriter(gzipStream);

                try
                {
                    while (hasMore = enumerator.MoveNext())
                    {
                        row = enumerator.Current;
                        for (col = 0; col < row.Length; col++)
                        {
                            columnTypes[col].Write(writer, row[col]);
                        }
                        counter++;

                        if (counter >= BatchSize)
                            break; // We've reached the batch size
                    }
                }
                catch (Exception e)
                {
                    throw new ClickHouseBulkCopySerializationException(row, col, e);
                }
            }

            token.ThrowIfCancellationRequested();
            stream.Seek(0, SeekOrigin.Begin);

            while (true)
            {
                var completedTaskIndex = Array.FindIndex(tasks, t => t.IsCompleted);
                if (completedTaskIndex >= 0)
                {
                    async Task SendBatch()
                    {
                        using (stream)
                        {
                            await connection.PostStreamAsync(useInlineQuery ? null : query, stream, true, token).ConfigureAwait(false);
                            Interlocked.Add(ref rowsWritten, counter);
                        }
                    }
                    tasks[completedTaskIndex] = SendBatch();
                    break; // while (true); go to next batch
                }
                else
                {
                    var completedTask = await Task.WhenAny(tasks).ConfigureAwait(false);
                    await completedTask.ConfigureAwait(false);
                }
            }
        }
        while (hasMore);

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (ownsConnection)
        {
            connection?.Dispose();
            ownsConnection = false;
        }
        GC.SuppressFinalize(this);
    }

    private static string GetColumnsExpression(IReadOnlyCollection<string> columns) => columns == null || columns.Count == 0 ? "*" : string.Join(",", columns);
}
