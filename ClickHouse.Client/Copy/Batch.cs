using System;
using System.Buffers;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Copy;

// Convenience argument collection
internal struct Batch : IDisposable
{
    public Memory<object>[] Rows;
    public int Size;
    public string Query;
    public ClickHouseType[] Types;

    public void Dispose()
    {
        if (Rows != null)
        {
            ArrayPool<Memory<object>>.Shared.Return(Rows, true);
            Rows = null;
        }
    }
}
