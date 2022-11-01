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
        private static readonly HttpClientHandler DefaultHttpClientHandler = new() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };

        public TimeSpan Timeout { get; init; }

        public HttpClient CreateClient(string name) => new(DefaultHttpClientHandler, false) { Timeout = Timeout };
    }
}
