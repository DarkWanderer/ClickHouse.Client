using System;
using System.Net;
using System.Net.Http;

namespace ClickHouse.Client.Http
{
    internal class SingleConnectionHttpClientFactory : IHttpClientFactory, IDisposable
    {
        private readonly HttpClientHandler handler;

        public TimeSpan Timeout { get; init; }

        public SingleConnectionHttpClientFactory()
        {
            handler = new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                MaxConnectionsPerServer = 1,
            };
        }

        public HttpClient CreateClient(string name) => new(handler, false)
        {
            Timeout = Timeout,
        };

        public void Dispose() => handler.Dispose();
    }
}
