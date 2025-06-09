using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Diagnostic;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Json;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO;

#if NET5_0_OR_GREATER
public class ClickHouseCancelableCommandRunner
{
    private string queryId;

    public ClickHouseCancelableCommandRunner()
    {
        queryId = Guid.NewGuid().ToString("N");
    }

    /// <summary>
    /// Gets QueryId associated with command
    /// </summary>
    public string QueryId => queryId;

    public QueryStats QueryStats { get; private set; }

    public async Task<HttpResponseMessage> PostSqlQueryAsync(ClickHouseCancelableCommand command, string sqlQuery, CancellationToken token)
    {
        if (command.ClickHouseConnection == null)
            throw new InvalidOperationException("Connection not set");
        using var activity = command.ClickHouseConnection.StartActivity("PostSqlQueryAsync");

        var uriBuilder = command.ClickHouseConnection.CreateUriBuilder();
        await command.ClickHouseConnection.EnsureOpenAsync().ConfigureAwait(false); // Preserve old behavior

        uriBuilder.QueryId = QueryId;
        uriBuilder.CommandQueryStringParameters = command.CustomSettings;

        using var postMessage = command.ClickHouseConnection.UseFormDataParameters
            ? BuildHttpRequestMessageWithFormData(
                command: command,
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder)
            : BuildHttpRequestMessageWithQueryParams(
                command: command,
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder);

        activity.SetQuery(sqlQuery);

        var response = await command.ClickHouseConnection.HttpClient
            .SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, token)
            .ConfigureAwait(false);

        QueryStats = ExtractQueryStats(response);
        activity.SetQueryStats(QueryStats);
        return await ClickHouseConnection.HandleError(response, sqlQuery, activity).ConfigureAwait(false);
    }

    private static HttpRequestMessage BuildHttpRequestMessageWithQueryParams(ClickHouseCancelableCommand command, string sqlQuery, ClickHouseUriBuilder uriBuilder)
    {
        if (command.ClickHouseParameters != null)
        {
            sqlQuery = command.ClickHouseParameters.ReplacePlaceholders(sqlQuery);
            foreach (ClickHouseDbParameter parameter in command.ClickHouseParameters)
            {
                uriBuilder.AddSqlQueryParameter(
                    parameter.ParameterName,
                    HttpParameterFormatter.Format(parameter, command.ClickHouseConnection.TypeSettings));
            }
        }

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        command.ClickHouseConnection.AddDefaultHttpHeaders(postMessage.Headers);
        HttpContent content = new StringContent(sqlQuery);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
        if (command.ClickHouseConnection.UseCompression)
        {
            content = new CompressedContent(content, DecompressionMethods.GZip);
        }

        postMessage.Content = content;

        return postMessage;
    }

    private static HttpRequestMessage BuildHttpRequestMessageWithFormData(ClickHouseCancelableCommand command, string sqlQuery, ClickHouseUriBuilder uriBuilder)
    {
        var content = new MultipartFormDataContent();

        if (command.ClickHouseParameters != null)
        {
            sqlQuery = command.ClickHouseParameters.ReplacePlaceholders(sqlQuery);

            foreach (ClickHouseDbParameter parameter in command.ClickHouseParameters)
            {
                content.Add(
                    content: new StringContent(HttpParameterFormatter.Format(parameter, command.ClickHouseConnection.TypeSettings)),
                    name: $"param_{parameter.ParameterName}");
            }
        }

        content.Add(
            content: new StringContent(sqlQuery),
            name: "query");

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        command.ClickHouseConnection.AddDefaultHttpHeaders(postMessage.Headers);

        postMessage.Content = content;

        return postMessage;
    }

    private static readonly JsonSerializerOptions SummarySerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = new SnakeCaseNamingPolicy(),
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString,
    };

    private static QueryStats ExtractQueryStats(HttpResponseMessage response)
    {
        try
        {
            const string summaryHeader = "X-ClickHouse-Summary";
            if (response.Headers.Contains(summaryHeader))
            {
                var value = response.Headers.GetValues(summaryHeader).FirstOrDefault();
                var jsonDoc = JsonDocument.Parse(value);
                return JsonSerializer.Deserialize<QueryStats>(value, SummarySerializerOptions);
            }
        }
        catch
        {
        }
        return null;
    }
}
#endif
