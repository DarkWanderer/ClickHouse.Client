using System;

namespace ClickHouse.Client.Copy;

public class ClickHouseBulkCopySerializationException : Exception
{
    public ClickHouseBulkCopySerializationException(object[] row, Exception innerException)
        : base("Error when serializing data", innerException)
    {
        Row = row;
    }

    /// <summary>
    /// Gets row at which exception happened
    /// </summary>
    public object[] Row { get; }
}
