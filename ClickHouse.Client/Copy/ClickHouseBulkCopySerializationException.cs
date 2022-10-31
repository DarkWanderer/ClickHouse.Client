using System;

namespace ClickHouse.Client.Copy;

public class ClickHouseBulkCopySerializationException : Exception
{
    public ClickHouseBulkCopySerializationException(object[] row, int index, Exception innerException)
        : base("Error when serializing data", innerException)
    {
        Row = row;
        Index = index;
    }

    /// <summary>
    /// Gets row at which exception happened
    /// </summary>
    public object[] Row { get; }

    /// <summary>
    /// Gets index of bad value in row
    /// </summary>
    public int Index { get; }
}
