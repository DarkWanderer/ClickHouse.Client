using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Utility;
using NUnit.Framework;

#if NET7_0_OR_GREATER

namespace ClickHouse.Client.Tests.ADO;

public class ConnectionCancelableTests : AbstractConnectionTestFixture
{
    #region IHttpClientFactory
    internal class TestException : Exception
    {
        public string Parameter { get; private set; }
        public TestException(string parameter) : base()
        {
            Parameter = parameter;
        }
    }

    internal class HttpClientFactoryFake : IHttpClientFactory
    {
        public HttpClient CreateClient(string name)
        {
            throw new TestException($"HttpClientFactoryFake:CreateClient: {name}");
        }
    }
    #endregion

    [Test]
    public async Task ShouldCreateCancelableConnectionWithProvidedHttpClient()
    {
        using var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
        using var httpClient = new HttpClient(httpClientHandler);
        using var connection = new ClickHouseCancelableConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
        await connection.OpenAsync();
        ClassicAssert.IsNotEmpty(connection.ServerVersion);
    }

    [Test]
    public void ShouldCreateCancelableConnectionWithProvidedHttpClientName()
    {
        using var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
        using var httpClient = new HttpClient(httpClientHandler);
        using var connection = new ClickHouseCancelableConnection(TestUtilities.GetConnectionStringBuilder().ToString(), new HttpClientFactoryFake(), "TestMe");
        Assert.Catch<TestException>(() => connection.Open(), "HttpClientFactoryFake:CreateClient: TestMe");
    }

    [Test]
    public void ShouldCreateCommandCancelable()
    {
        using var connection = new ClickHouseCancelableConnection();
        var command1 = connection.CreateCommand();
        Assert.That(command1.GetType(), Is.EqualTo(typeof(ClickHouseCancelableCommand)));
        Assert.That(command1.ClickHouseConnection, Is.EqualTo(connection));
    }

    // see: https://github.com/linq2db/linq2db/discussions/4966
    // see: https://github.com/DarkWanderer/ClickHouse.Client/issues/489
    [Test]
    [Explicit("In linq2db you may call the same command multiple times in different SQLs")]
    public async Task MultiTimeCallOneCommandAndFallExceptionWhenHasQueryId()
    {
        string queryId = "MyQueryId123456";
        var command = cancelableConnection.CreateCommand();
        command.QueryId = queryId;

        try
        {
            List<Task> tasks = new List<Task>(2);

            command.CommandText = "SELECT sleep(2) FROM system.numbers LIMIT 100";
            tasks.Add(command.ExecuteScalarAsync());

            command.CommandText = "SELECT sleep(3) FROM system.numbers LIMIT 200"; // this is another query with the same DbCommand/QueryId
            tasks.Add(command.ExecuteScalarAsync());

            await Task.WhenAll(tasks);
        }
        catch (ClickHouseServerException ex) when (ex.ErrorCode == 216)
        {
            Assert.Fail("The query id is running.");
        }
        catch (Exception)
        {
            Assert.Fail("Query throw another exception");
        }
    }

    // see: https://github.com/DarkWanderer/ClickHouse.Client/discussions/482
    [Test]
    [Explicit("Support Cancellation")]
    public async Task SupportCancellation()
    {
        string queryId = "MyQueryId123456";
        var command = cancelableConnection.CreateCommand();
        command.CommandText = "SELECT *\r\nFROM (SELECT sleep(3), '0' as num FROM system.numbers LIMIT 100) t1\r\nINNER JOIN (SELECT sleep(3), '0' as num FROM system.numbers LIMIT 100) t2 on t1.num = t2.num";
        command.QueryId = queryId;

        var commandRunning = connection.CreateCommand();
        commandRunning.CommandText = $"SELECT count(*) FROM system.processes where query_id like '{queryId}';";

        CancellationTokenSource cts = new CancellationTokenSource();

        async Task cancelAsync(CancellationTokenSource cancellationTokenSource)
        {
            await Task.Delay(1000);
            cancellationTokenSource.Cancel();
        }

        Stopwatch sw = Stopwatch.StartNew();
        try
        {
            _ = Task.Run(async () => await cancelAsync(cts));

            await command.ExecuteScalarAsync(cts.Token);
            sw.Stop();

            if (sw.ElapsedMilliseconds > 5000)
                Assert.Fail("The query was not cancelled in time");

            Assert.Fail("The query did not throw an exception");
        }
        catch (OperationCanceledException)
        {
            // Expected exception as operation canceled

            ulong num = (ulong)commandRunning.ExecuteScalar();
            if (num > 0)
                Assert.Fail("The query was not cancelled in time, it is still running on the server");
        }
        catch (Exception)
        {
            Assert.Fail("Query throw another exception");
        }
        sw.Stop();
    }
}
#endif
