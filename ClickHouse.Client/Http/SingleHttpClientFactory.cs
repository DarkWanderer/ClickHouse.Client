using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Client.Http
{
    internal class SingleHttpClientFactory : IHttpClientFactory, IDisposable
    {
        private readonly HttpClient client;

        public SingleHttpClientFactory(HttpClient client)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public SingleHttpClientFactory(TimeSpan timeout)
        {
            client = new(new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate }, true)
            {
                Timeout = timeout,
            };
        }

        public HttpClient CreateClient(string name) => client;

        public void Dispose() => client.Dispose();
    }
}
