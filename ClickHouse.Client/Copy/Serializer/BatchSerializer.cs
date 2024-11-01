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
            RowBinaryFormat.RowBinary => RowBinary,
            RowBinaryFormat.RowBinaryWithDefaults => RowBinaryWithDefaults,
            _ => throw new NotSupportedException(format.ToString())
        };
    }

    private static readonly BatchSerializer RowBinary = new(new RowBinarySerializer());
    private static readonly BatchSerializer RowBinaryWithDefaults = new(new RowBinaryWithDefaultsSerializer());

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

        object[] row = null;
        int counter = 0;
        var enumerator = batch.Rows.GetEnumerator();
        try
        {
            while (enumerator.MoveNext())
            {
                row = (object[])enumerator.Current;
                rowSerializer.Serialize(row, batch.Types, writer);

                counter++;
                if (counter >= batch.Size)
                    break; // We've reached the batch size
            }
        }
        catch (Exception e)
        {
            throw new ClickHouseBulkCopySerializationException(row, e);
        }
    }
}
