using System;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class ConnectionTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldCreateConnectionWithProvidedHttpClient()
    {
        using var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
        using var httpClient = new HttpClient(httpClientHandler);
        using var connection = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
        await connection.OpenAsync();
        ClassicAssert.IsNotEmpty(connection.ServerVersion);
    }

    [Test]
    public void ShouldThrowExceptionOnInvalidHttpClient()
    {
        using var httpClient = new HttpClient(); // No decompression handler
        using var connection = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
        Assert.Throws<InvalidOperationException>(() => connection.Open());
    }

    [Test]
    public void ShouldParseCustomParameter()
    {
        using var connection = new ClickHouseConnection("set_my_parameter=aaa");
        Assert.That(connection.CustomSettings["my_parameter"], Is.EqualTo("aaa"));
    }

    [Test]
    public void ShouldEmitCustomParameter()
    {
        using var connection = new ClickHouseConnection();
        connection.CustomSettings.Add("my_parameter", "aaa");
        Assert.That(connection.ConnectionString, Contains.Substring("set_my_parameter=aaa"));
    }

    [Test]
    public void ShouldConnectToServer()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection();
        connection.Open();
        ClassicAssert.IsNotEmpty(connection.ServerVersion);
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));
        connection.Close();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
    }

    [Test]
    [TestCase("1.2.3.4.altinity")]
    [TestCase("1.2.3.4")]
    [TestCase("20")]
    [TestCase("20.1")]
    [TestCase("20.1.2")]
    public void ShoulParseVersion(string version) => _ = ClickHouseConnection.ParseVersion(version);

    [Test]
    public async Task TimeoutShouldCancelConnection()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = false;
        builder.Compression = true;
        builder.Timeout = TimeSpan.FromMilliseconds(5);
        var connection = new ClickHouseConnection(builder.ToString());
        try
        {
            var task = connection.ExecuteScalarAsync("SELECT sleep(1)");
            _ = await task;
            Assert.Fail("The task should have been cancelled before completion");
        }
        catch (TaskCanceledException)
        {
            /* Expected: task cancelled */
        }
    }

    [Test]
    public async Task ServerShouldSetQueryId()
    {
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        await command.ExecuteScalarAsync();
        ClassicAssert.IsFalse(string.IsNullOrWhiteSpace(command.QueryId));
    }

    [Test]
    public async Task ClientShouldSetQueryId()
    {
        string queryId = "MyQueryId123456";
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.QueryId = queryId;
        await command.ExecuteScalarAsync();
        Assert.That(command.QueryId, Is.EqualTo(queryId));
    }

    [Test]
    [Explicit("This test takes 3s, and can be flaky on loaded server")]
    public async Task ReplaceRunningQuerySettingShouldReplace()
    {
        connection.CustomSettings.Add("replace_running_query", 1);
        string queryId = "MyQueryId123456";

        var command1 = connection.CreateCommand();
        var command2 = connection.CreateCommand();

        command1.CommandText = "SELECT sleep(3) FROM system.numbers LIMIT 100";
        command2.CommandText = "SELECT 1";

        command1.QueryId = queryId;
        command2.QueryId = queryId;

        var asyncResult1 = command1.ExecuteScalarAsync();
        var asyncResult2 = command2.ExecuteScalarAsync();

        try
        {
            await asyncResult1;
            Assert.Fail("Query was not cancelled as expected");
        }
        catch (ClickHouseServerException ex) when (ex.ErrorCode == 394)
        {
            // Expected exception as next query replaced this one
        }
        await asyncResult2;
    }

    [Test]
    [Ignore("TODO")]
    public void ShouldFetchSchema()
    {
        var schema = connection.GetSchema();
        ClassicAssert.NotNull(schema);
    }

    [Test]
    [Ignore("TODO")]
    public void ShouldFetchSchemaTables()
    {
        var schema = connection.GetSchema("Tables");
        ClassicAssert.NotNull(schema);
    }

    [Test]
    [Ignore("Needs support for named tuple parameters")]
    public void ShouldFetchSchemaDatabaseColumns()
    {
        var schema = connection.GetSchema("Columns", ["system"]);
        ClassicAssert.NotNull(schema);
        Assert.That(new[] { "Database", "Table", "DataType", "ProviderType" }, Is.SubsetOf(GetColumnNames(schema)));
    }

    [Test]
    public void ShouldFetchSchemaTableColumns()
    {
        var schema = connection.GetSchema("Columns", ["system", "functions"]);
        ClassicAssert.NotNull(schema);
        Assert.That(new[] { "Database", "Table", "DataType", "ProviderType" }, Is.SubsetOf(GetColumnNames(schema)));
    }

    [Test]
    public void ChangeDatabaseShouldChangeDatabase()
    {
        // Using separate connection instance here to avoid conflicting with other tests
        using var conn = TestUtilities.GetTestClickHouseConnection();
        conn.ChangeDatabase("system");
        Assert.That(conn.Database, Is.EqualTo("system"));
        conn.ChangeDatabase("default");
        Assert.That(conn.Database, Is.EqualTo("default"));
    }

    [Test]
    public void ShouldExcludePasswordFromRedactedConnectionString()
    {
        const string MOCK = "verysecurepassword";
        using var conn = TestUtilities.GetTestClickHouseConnection();
        var builder = conn.ConnectionStringBuilder;
        builder.Password = MOCK;
        conn.ConnectionStringBuilder = builder;
        Assert.Multiple(() =>
        {
            Assert.That(conn.ConnectionString, Contains.Substring($"Password={MOCK}"));
            Assert.That(conn.RedactedConnectionString, Is.Not.Contains($"Password={MOCK}"));
        });
    }

    [Test]
    [TestCase("https")]
    [TestCase("http")]
    public void ShouldSaveProtocolAtConnectionString(string protocol)
    {
        string protocolPart = $"Protocol={protocol}";
        string connString = new ClickHouseConnectionStringBuilder(protocolPart).ToString();
        using var conn = new ClickHouseConnection(connString);
        Assert.That(conn.ConnectionString, Contains.Substring(protocolPart));
    }

    [Test]
    public async Task ShouldPostDynamicallyGeneratedRawStream()
    {
        var targetTable = "test.raw_stream";

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Int32) ENGINE Null");
        await connection.PostStreamAsync($"INSERT INTO {targetTable} FORMAT CSV", async (stream, ct) =>
        {

            foreach (var i in Enumerable.Range(1, 1000))
            {
                var line = $"{i}\n";
                var bytes = Encoding.UTF8.GetBytes(line);
                await stream.WriteAsync(bytes, 0, bytes.Length, ct);
            }
        }, false, CancellationToken.None);
    }

    private static string[] GetColumnNames(DataTable table) => table.Columns.Cast<DataColumn>().Select(dc => dc.ColumnName).ToArray();
}
