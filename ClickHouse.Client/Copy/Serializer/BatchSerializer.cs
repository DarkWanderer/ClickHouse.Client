using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Copy.Serializer;

internal class BatchSerializer : IBatchSerializer
{
    public static BatchSerializer GetByRowBinaryFormat(RowBinaryFormat format)
    {
        return format switch
        {
            RowBinaryFormat.RowBinary => new BatchSerializer(new RowBinarySerializer()),
            RowBinaryFormat.RowBinaryWithDefaults => new BatchSerializer(new RowBinaryWithDefaultsSerializer()),
            _ => throw new NotSupportedException(format.ToString())
        };
    }

    private readonly IRowSerializer rowSerializer;

    public BatchSerializer(IRowSerializer rowSerializer)
    {
        this.rowSerializer = rowSerializer;
    }

    public void Serialize(Batch batch, Stream stream)
    {
        using var gzipStream = new BufferedStream(new GZipStream(stream, CompressionLevel.Fastest, true), 256 * 1024);
        using (var textWriter = new StreamWriter(gzipStream, Encoding.UTF8, 4 * 1024, true))
        {
            textWriter.WriteLine(batch.Query);
        }

        using var writer = new ExtendedBinaryWriter(gzipStream);

        Memory<object> row = null;
        var enumerator = batch.Rows.Memory.Span.Slice(0, batch.Size).GetEnumerator();
        try
        {
            while (enumerator.MoveNext())
            {
                row = enumerator.Current;
                rowSerializer.Serialize(row.Span, batch.Types, writer);
            }
        }
        catch (Exception e)
        {
            throw new ClickHouseBulkCopySerializationException(row.ToArray(), e);
        }
    }
}
