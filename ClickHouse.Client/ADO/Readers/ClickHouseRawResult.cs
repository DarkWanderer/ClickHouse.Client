using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseRawResult : IDisposable
    {
        private readonly HttpResponseMessage response;

        public ClickHouseRawResult(HttpResponseMessage response)
        {
            this.response = response;
        }

        public Task<Stream> ReadAsStreamAsync() => response.Content.ReadAsStreamAsync();

        public void Dispose() => response?.Dispose();
    }
}
