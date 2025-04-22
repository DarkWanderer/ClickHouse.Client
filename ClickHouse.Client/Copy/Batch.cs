using System;
using System.Buffers;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy;

// Convenience argument collection
internal struct Batch : IDisposable
{
    public IMemoryOwner<Memory<object>> Rows;
    public int Size;
    public string Query;
    public ClickHouseType[] Types;

    public void Dispose()
    {
        if (Rows != null)
        {
            Rows.Dispose();
            Rows = null;
        }
    }
}
