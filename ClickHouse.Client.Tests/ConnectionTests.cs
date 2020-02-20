using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
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
    }
}