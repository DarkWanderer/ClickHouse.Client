using System;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ConnectionTests : AbstractConnectionTestFixture
    {
        [Test]
        public async Task ShouldCreateConnectionWithProvidedHttpClient()
        {
            using var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
            using var httpClient = new HttpClient(httpClientHandler);
            using var connection = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
            await connection.OpenAsync();
            Assert.IsNotEmpty(connection.ServerVersion);
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
            Assert.AreEqual("aaa", connection.CustomSettings["my_parameter"]);
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
            Assert.IsNotEmpty(connection.ServerVersion);
            Assert.AreEqual(ConnectionState.Open, connection.State);
            connection.Close();
            Assert.AreEqual(ConnectionState.Closed, connection.State);
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
            Assert.IsFalse(string.IsNullOrWhiteSpace(command.QueryId));
        }

        [Test]
        public async Task ClientShouldSetQueryId()
        {
            string queryId = "MyQueryId123456";
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            command.QueryId = queryId;
            await command.ExecuteScalarAsync();
            Assert.AreEqual(queryId, command.QueryId);
        }

        [Test]
        [Explicit("This test takes 3s, and can be flaky on loaded server")]
        public async Task ReplaceRunningQuerySettingShouldReplace()
        {
            using var connection = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString());
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
            Assert.IsNotNull(schema);
        }

        [Test]
        [Ignore("TODO")]
        public void ShouldFetchSchemaTables()
        {
            var schema = connection.GetSchema("Tables");
            Assert.IsNotNull(schema);
        }

        [Test]
        public void ShouldFetchSchemaDatabaseColumns()
        {
            var schema = connection.GetSchema("Columns", new[] { "system" });
            Assert.IsNotNull(schema);
            CollectionAssert.IsSubsetOf(new[] { "Database", "Table", "DataType", "ProviderType" }, GetColumnNames(schema));
        }

        [Test]
        public void ShouldFetchSchemaTableColumns()
        {
            var schema = connection.GetSchema("Columns", new[] { "system", "functions" });
            Assert.IsNotNull(schema);
            CollectionAssert.IsSubsetOf(new[] { "Database", "Table", "DataType", "ProviderType" }, GetColumnNames(schema));
        }

        [Test]
        public void ChangeDatabaseShouldChangeDatabase()
        {
            // Using separate connection instance here to avoid conflicting with other tests
            using var conn = TestUtilities.GetTestClickHouseConnection();
            conn.ChangeDatabase("system");
            Assert.AreEqual("system", conn.Database);
            conn.ChangeDatabase("default");
            Assert.AreEqual("default", conn.Database);
        }

        private static string[] GetColumnNames(DataTable table) => table.Columns.Cast<DataColumn>().Select(dc => dc.ColumnName).ToArray();
    }
}
