using System;
using System.Net.Http;

namespace ClickHouse.Client.Utility;

internal class CannedHttpClientFactory : IHttpClientFactory
{
    private readonly HttpClient httpClient;

    public CannedHttpClientFactory(HttpClient httpClient)
    {
        this.httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    public HttpClient CreateClient(string name) => httpClient;
}
