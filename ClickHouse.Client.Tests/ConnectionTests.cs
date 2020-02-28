using System;
using System.Data;
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
            using var response = await connection.PostSqlQueryAsync("SELECT 1", CancellationToken.None);
            var result = await response.Content.ReadAsStringAsync();
            Assert.AreEqual("1", result.Trim());
        }

        [Test]
        public async Task TimeoutShouldCancelConnection()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.UseSession = false;
            builder.Driver = ClickHouseConnectionDriver.Binary;
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
    }
}
