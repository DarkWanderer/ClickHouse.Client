using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Client.Http
{
    internal class CannedHttpClientFactory : IHttpClientFactory
    {
        private readonly HttpClient client;

        public CannedHttpClientFactory(HttpClient client)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public HttpClient CreateClient(string name) => client;
    }
}
