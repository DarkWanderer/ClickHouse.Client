using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Client.Utility
{
    internal static class HttpClientExtensions
    {
        public static async Task<HttpResponseMessage> SendAsyncWithTimeout(this HttpClient httpClient, HttpRequestMessage request, HttpCompletionOption completionOption, CancellationToken cancellationToken, TimeSpan timeout)
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
            return await httpClient.SendAsync(request, completionOption, combinedCts.Token).ConfigureAwait(false);
        }
    }
}
