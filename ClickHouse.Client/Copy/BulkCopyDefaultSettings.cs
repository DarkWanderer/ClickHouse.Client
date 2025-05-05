using Microsoft.IO;

namespace ClickHouse.Client.Copy;

internal static class BulkCopyDefaultSettings
{
    public static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();
    public static readonly int BatchSize = 100000;
    public static readonly int MaxDegreeOfParallelism = 4;
    public static readonly RowBinaryFormat RowBinaryFormat = RowBinaryFormat.RowBinary;
}
