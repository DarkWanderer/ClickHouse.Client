
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Client.ADO;

/// <summary>
/// HttpContent implementation allowing streaming large payloads without having to materialize
/// the entire stream up-front.
/// </summary>
internal class StreamCallbackContent : HttpContent
{
    private readonly Func<Stream, CancellationToken, Task> callback;
    private readonly CancellationToken cancellationToken;

    public StreamCallbackContent(Func<Stream, CancellationToken, Task> callback, CancellationToken cancellationToken)
    {
        this.callback = callback;
        this.cancellationToken = cancellationToken;
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
    {
        return callback(stream, cancellationToken);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }
}
