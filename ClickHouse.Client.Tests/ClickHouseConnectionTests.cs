using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ClickHouseConnectionTests
    {
        [Test]
        public void ShouldConnectToServer()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(default);
            connection.Open();
            Assert.IsNotEmpty(connection.ServerVersion);
            connection.Close();
        }

        [Test]
        public async Task ShouldGetQueryAsync()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(default);
            using var response = await connection.GetSqlQueryAsync("SELECT 1", CancellationToken.None);
            var result = await response.Content.ReadAsStringAsync();
            Assert.AreEqual("1", result.Trim());
        }

        [Test]
        public async Task ShouldPostQueryAsync()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(default);
            using var response = await connection.PostSqlQueryAsync("SELECT 1", CancellationToken.None);
            var result = await response.Content.ReadAsStringAsync();
            Assert.AreEqual("1", result.Trim());
        }
    }
}