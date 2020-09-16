using System;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ConnectionTests
    {
        private readonly ClickHouseConnection connection = TestUtilities.GetTestClickHouseConnection(default);

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
            connection.Open();
            Assert.IsNotEmpty(connection.ServerVersion);
            Assert.AreEqual(ConnectionState.Open, connection.State);
            connection.Close();
            Assert.AreEqual(ConnectionState.Closed, connection.State);
        }

        [Test]
        public async Task ShouldPostQueryAsync()
        {
            using var response = await connection.PostSqlQueryAsync("SELECT 1 FORMAT TabSeparated", CancellationToken.None);
            var result = await response.Content.ReadAsStringAsync();
            Assert.AreEqual("1", result.Trim());
        }

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

        private static string[] GetColumnNames(DataTable table) => table.Columns.Cast<DataColumn>().Select(dc => dc.ColumnName).ToArray();
    }
}
