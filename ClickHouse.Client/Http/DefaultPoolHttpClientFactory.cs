using System;
using System.Net;
using System.Net.Http;

namespace ClickHouse.Client.Http;

internal class DefaultPoolHttpClientFactory : IHttpClientFactory
{
    private static readonly HttpClientHandler DefaultHttpClientHandler = new() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };

    public TimeSpan Timeout { get; init; }

    public HttpClient CreateClient(string name) => new(DefaultHttpClientHandler, false) { Timeout = Timeout };
}
