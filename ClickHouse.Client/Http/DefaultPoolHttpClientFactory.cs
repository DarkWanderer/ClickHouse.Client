using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Client.Http
{
    internal class DefaultPoolHttpClientFactory : IHttpClientFactory
    {
        public TimeSpan Timeout { get; init; }

        private static readonly HttpClientHandler DefaultHttpClientHandler = new() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };

        public HttpClient CreateClient(string name) => new(DefaultHttpClientHandler, false) { Timeout = Timeout };
    }
}
