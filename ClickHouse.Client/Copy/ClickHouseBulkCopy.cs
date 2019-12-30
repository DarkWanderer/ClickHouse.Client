using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy
{
    public class ClickHouseBulkCopy : IDisposable
    {
        private readonly ClickHouseConnection connection;
        private long rowsWritten = 0;

        public ClickHouseBulkCopy(ClickHouseConnection connection)
        {
            this.connection = connection;
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

            return WriteToServerAsync(AsEnumerable(reader), token);
        }

        public Task WriteToServerAsync(IEnumerable<object[]> rows) => WriteToServerAsync(rows, CancellationToken.None);

        public async Task WriteToServerAsync(IEnumerable<object[]> rows, CancellationToken token)
        {
            if (rows is null)
                throw new ArgumentNullException(nameof(rows));
            if (string.IsNullOrWhiteSpace(DestinationTableName))
                throw new InvalidOperationException(Resources.DestinationTableNotSetMessage);

            var tableColumns = await GetTargetTableSchemaAsync(token);

            var batchBlock = new BatchBlock<object[]>(BatchSize, new GroupingDataflowBlockOptions { CancellationToken = token });
            var actionBlock = new ActionBlock<object[][]>(
                block => PushBatch(block, tableColumns, token),
                new ExecutionDataflowBlockOptions
                {
                    CancellationToken = token,
                    MaxDegreeOfParallelism = MaxDegreeOfParallelism
                });

            batchBlock.LinkTo(actionBlock);
            _ = batchBlock.Completion.ContinueWith(task => actionBlock.Complete());

            foreach (var row in rows)
            {
                token.ThrowIfCancellationRequested();
                batchBlock.Post(row);
            }
            batchBlock.Complete();
            await actionBlock.Completion;
        }

        private static IEnumerable<object[]> AsEnumerable(IDataReader reader)
        {
            while (reader.Read())
            {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                yield return values;
            }
        }

        private async Task PushBatch(object[][] values, ClickHouseType[] columnTypes, CancellationToken token)
        {
            using var stream = new MemoryStream();
            using var writer = new ExtendedBinaryWriter(stream);
            using var streamer = new BinaryStreamWriter(writer);
            foreach (object[] row in values)
            {
                for (int i = 0; i < row.Length; i++)
                {
                    streamer.WriteValue(row[i], columnTypes[i]);
                }
            }
            stream.Seek(0, SeekOrigin.Begin);

            var query = $"INSERT INTO {DestinationTableName} FORMAT RowBinary";
            var result = await connection.PostDataAsync(query, stream, token).ConfigureAwait(false);
            Interlocked.Add(ref rowsWritten, values.Length);
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
